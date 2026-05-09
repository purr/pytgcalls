class CallSources:
    """
    Tracks which incoming-video subscriptions we currently hold per
    participant for a single group call.

    Public dicts (`camera`, `presentation`) keep the original shape
    (``user_id -> endpoint_string``) so any external reader continues to
    work unchanged.

    Private parallel dicts (`_camera_sig`, `_presentation_sig`) hold a
    stable diff signature ``(endpoint, ssrc_groups_repr)`` per user.
    The reconciliation logic in ``_update_sources`` and in the live MTProto
    update handler keys subscription decisions off these signatures so that
    a same-user republish under a NEW endpoint, or a simulcast renegotiation
    that changes ssrc groups, correctly tears down the old binding and
    establishes the new one.  Without the signature dicts, the previous
    "user_id present in cache?" check would silently skip the re-subscribe
    and the affected participant's video would freeze for the rest of the
    session.
    """

    def __init__(self):
        self.camera = dict()
        self.presentation = dict()
        # Diff signature: user_id -> (endpoint: str, sources_repr: tuple)
        self._camera_sig = dict()
        self._presentation_sig = dict()
