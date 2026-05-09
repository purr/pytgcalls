from typing import Union

from ntgcalls import ConnectionError
from ntgcalls import ConnectionNotFound

from ...mtproto import BridgedClient
from ...scaffold import Scaffold
from ...types.calls import CallSources
from ._video_sig import video_signature


class UpdateSources(Scaffold):
    async def _update_sources(
        self,
        chat_id: Union[int, str],
    ):
        """
        Reconcile our cache of incoming-video subscriptions against the
        live participants list reported by ``phone.GetGroupParticipants``.

        Idempotent: a second call with no participant changes issues
        zero ntgcalls add/remove calls.

        Re-subscribes correctly when:
          * a user newly appears with a video stream;
          * a user's endpoint changes (republish, network handover);
          * a user's ssrc-groups change (simulcast renegotiation);
          * a user no longer publishes video.

        The previous implementation keyed off ``user_id in
        _call_sources[chat_id].camera`` which silently skipped the
        re-subscribe whenever the user_id was already present, even if
        the underlying endpoint had changed.  ntgcalls'
        ``addIncomingVideo`` is a no-op when called for an endpoint
        already on file, so the re-subscribe must always be a
        ``remove_incoming_video(old) + add_incoming_video(new)`` pair.
        """
        # PRECONDITION: callers must already hold ``self._chat_lock`` for
        # ``chat_id`` so we serialize against the live MTProto handler at
        # ``HandleMTProtoUpdates._handle_mtproto_updates`` (which also
        # mutates ``_call_sources[chat_id]``).  All current callers do:
        #
        #   * ``methods/stream/play.py::play`` is decorated with
        #     ``@mutex`` (see ``pytgcalls/mutex.py``) which acquires the
        #     same lock before invoking the body.
        #   * ``methods/internal/switch_connection.py::_switch_connection``
        #     is invoked from the handler at
        #     ``handle_mtproto_updates.py:179`` inside its own
        #     ``async with await self._chat_lock.acquire(chat_id):``.
        #
        # We deliberately do NOT acquire the lock inside this method:
        # ``WaitCounterLock`` wraps a non-reentrant ``asyncio.Lock``, so a
        # nested acquire from a holder of the same lock would deadlock.
        # Future callers MUST follow the same convention.
        participants = await self._app.get_group_call_participants(
            chat_id,
        )
        if chat_id not in self._call_sources:
            self._call_sources[chat_id] = CallSources()
        cs = self._call_sources[chat_id]

        # Snapshot the live state as user_id -> (info, sig).  ``info`` is
        # the original VideoInfo/PresentationInfo we hand to
        # ``add_incoming_video``; ``sig`` is the hashable diff key.
        desired_camera = {}
        desired_screen = {}
        for p in participants:
            new_cam_sig = video_signature(p.video_info)
            if new_cam_sig is not None:
                desired_camera[p.user_id] = (p.video_info, new_cam_sig)
            new_scr_sig = video_signature(p.presentation_info)
            if new_scr_sig is not None:
                desired_screen[p.user_id] = (
                    p.presentation_info, new_scr_sig,
                )
            if p.user_id == BridgedClient.chat_id(
                self._cache_local_peer,
            ) and p.muted_by_admin:
                self._need_unmute.add(chat_id)

        await self._reconcile_video_kind(
            chat_id, cs.camera, cs._camera_sig, desired_camera,
        )
        await self._reconcile_video_kind(
            chat_id,
            cs.presentation,
            cs._presentation_sig,
            desired_screen,
        )

    async def _reconcile_video_kind(
        self,
        chat_id: Union[int, str],
        public_dict: dict,
        sig_dict: dict,
        desired: dict,
    ):
        """
        Apply the (remove old, add new) pair for entries whose signature
        changed, plus pure adds for new users and pure removes for users
        who dropped the stream.  ``public_dict`` and ``sig_dict`` are
        mutated in lockstep so external readers always see a consistent
        ``user_id -> endpoint`` map.
        """
        # 1. REMOVE (or rebind) entries whose signature changed or vanished.
        for user_id, prev_sig in list(sig_dict.items()):
            new = desired.get(user_id)
            if new is not None and new[1] == prev_sig:
                continue
            old_endpoint = public_dict.get(user_id)
            if old_endpoint is not None:
                try:
                    await self._binding.remove_incoming_video(
                        chat_id, old_endpoint,
                    )
                except (ConnectionNotFound, ConnectionError):
                    pass
            public_dict.pop(user_id, None)
            sig_dict.pop(user_id, None)

        # 2. ADD entries that are new or were just torn down for rebind.
        for user_id, (info, new_sig) in desired.items():
            if sig_dict.get(user_id) == new_sig:
                continue
            try:
                await self._binding.add_incoming_video(
                    chat_id, info.endpoint, info.sources,
                )
            except (ConnectionNotFound, ConnectionError):
                continue
            public_dict[user_id] = info.endpoint
            sig_dict[user_id] = new_sig
