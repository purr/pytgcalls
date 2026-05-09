import logging

from ntgcalls import ConnectionError
from ntgcalls import ConnectionNotFound

from ...exceptions import CallBusy
from ...exceptions import CallDeclined
from ...exceptions import CallDiscarded
from ...mtproto import BridgedClient
from ...scaffold import Scaffold
from ...types import CallData
from ...types import ChatUpdate
from ...types import GroupCallParticipant
from ...types import RawCallUpdate
from ...types import Update
from ...types import UpdatedGroupCallParticipant
from ._video_sig import video_signature

py_logger = logging.getLogger('pytgcalls')


class HandleMTProtoUpdates(Scaffold):
    async def _handle_mtproto_updates(self, update: Update):
        py_logger.debug('Received: %s', update)
        chat_id = update.chat_id
        if update.chat_id in self._p2p_configs:
            p2p_config = self._p2p_configs[chat_id]
            if not p2p_config.wait_data.done():
                if isinstance(update, RawCallUpdate):
                    if update.status & RawCallUpdate.Type.UPDATED_CALL:
                        p2p_config.wait_data.set_result(
                            update,
                        )
                if isinstance(update, ChatUpdate) and \
                        p2p_config.outgoing:
                    if update.status & ChatUpdate.Status.DISCARDED_CALL:
                        p2p_config.wait_data.set_exception(
                            CallBusy(
                                chat_id,
                            ) if update.status &
                            ChatUpdate.Status.BUSY_CALL else
                            CallDeclined(
                                chat_id,
                            ),
                        )
        if chat_id in self._wait_connect and \
                not self._wait_connect[chat_id].done() and \
                chat_id not in self._p2p_configs:
            if isinstance(update, ChatUpdate):
                if update.status & ChatUpdate.Status.DISCARDED_CALL:
                    self._wait_connect[chat_id].set_exception(
                        CallDiscarded(
                            chat_id,
                        ),
                    )
        if isinstance(update, RawCallUpdate):
            if update.status & RawCallUpdate.Type.REQUESTED:
                self._p2p_configs[chat_id] = CallData(
                    await self._app.get_dhc(),
                    self.loop,
                    update.g_a_or_b,
                )
                update = ChatUpdate(
                    chat_id,
                    ChatUpdate.Status.INCOMING_CALL,
                )
        if isinstance(update, RawCallUpdate):
            if update.status & RawCallUpdate.Type.SIGNALING_DATA:
                try:
                    await self._binding.send_signaling(
                        update.chat_id,
                        update.signaling_data,
                    )
                except (ConnectionNotFound, ConnectionError):
                    pass
        if isinstance(update, ChatUpdate):
            if update.status & ChatUpdate.Status.LEFT_CALL:
                await self._clear_call(chat_id)
        if isinstance(update, UpdatedGroupCallParticipant):
            participant = update.participant
            action = update.action
            chat_peer = self._cache_user_peer.get(chat_id)
            user_id = participant.user_id

            async with await self._chat_lock.acquire(chat_id):
                if chat_id in self._call_sources:
                    call_sources = self._call_sources[chat_id]

                    # Signature-based reconciliation.  See
                    # ``_video_sig.video_signature`` and
                    # ``UpdateSources._reconcile_video_kind`` for the full
                    # rationale; the short version is that the previous
                    # boolean ``was_camera != participant.video_camera``
                    # trigger silently skipped re-subscription whenever
                    # the camera flag stayed True but the underlying
                    # endpoint or ssrc-groups changed (e.g. republish or
                    # simulcast renegotiation), causing the affected
                    # participant's video to freeze for the rest of the
                    # session even though audio and the participant entry
                    # itself kept updating normally.
                    cam_sig = video_signature(participant.video_info)
                    desired_cam = (
                        {user_id: (participant.video_info, cam_sig)}
                        if cam_sig is not None else {}
                    )
                    # Limit reconciliation to this single user_id so we
                    # do not accidentally tear down OTHER users' subs in
                    # response to one participant's update.  Build a
                    # filtered view of the cache containing only this
                    # user's existing entries.
                    cam_pub_filtered = (
                        {user_id: call_sources.camera[user_id]}
                        if user_id in call_sources.camera else {}
                    )
                    cam_sig_filtered = (
                        {user_id: call_sources._camera_sig[user_id]}
                        if user_id in call_sources._camera_sig else {}
                    )
                    await self._reconcile_video_kind(
                        chat_id,
                        cam_pub_filtered,
                        cam_sig_filtered,
                        desired_cam,
                    )
                    # Mirror the filtered changes back to the master
                    # cache.  ``_reconcile_video_kind`` mutates the dicts
                    # we passed in lockstep; copy the (possibly absent)
                    # current value into the master.
                    if user_id in cam_pub_filtered:
                        call_sources.camera[user_id] = cam_pub_filtered[user_id]
                        call_sources._camera_sig[user_id] = cam_sig_filtered[user_id]
                    else:
                        call_sources.camera.pop(user_id, None)
                        call_sources._camera_sig.pop(user_id, None)

                    scr_sig = video_signature(participant.presentation_info)
                    desired_scr = (
                        {user_id: (participant.presentation_info, scr_sig)}
                        if scr_sig is not None else {}
                    )
                    scr_pub_filtered = (
                        {user_id: call_sources.presentation[user_id]}
                        if user_id in call_sources.presentation else {}
                    )
                    scr_sig_filtered = (
                        {user_id: call_sources._presentation_sig[user_id]}
                        if user_id in call_sources._presentation_sig else {}
                    )
                    await self._reconcile_video_kind(
                        chat_id,
                        scr_pub_filtered,
                        scr_sig_filtered,
                        desired_scr,
                    )
                    if user_id in scr_pub_filtered:
                        call_sources.presentation[user_id] = scr_pub_filtered[user_id]
                        call_sources._presentation_sig[user_id] = scr_sig_filtered[user_id]
                    else:
                        call_sources.presentation.pop(user_id, None)
                        call_sources._presentation_sig.pop(user_id, None)

            if chat_peer:
                is_self = BridgedClient.chat_id(
                    chat_peer,
                ) == participant.user_id if chat_peer else False
                if is_self:
                    if action == GroupCallParticipant.Action.KICKED or \
                            action == GroupCallParticipant.Action.LEFT:
                        await self._clear_call(chat_id)
                    if (
                        chat_id in self._need_unmute and
                        action == GroupCallParticipant.Action.UPDATED
                        and not participant.muted_by_admin
                    ):
                        await self._update_status(
                            chat_id,
                            await self._binding.get_state(chat_id),
                        )
                        await self._switch_connection(chat_id)

                    if (
                        participant.muted_by_admin and
                        action != GroupCallParticipant.Action.LEFT
                    ):
                        self._need_unmute.add(chat_id)
                    else:
                        self._need_unmute.discard(chat_id)
        if not isinstance(update, RawCallUpdate):
            await self._propagate(
                update,
                self,
            )
