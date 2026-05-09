"""
Shared helper for computing a stable diff signature on a participant's
incoming-video metadata (``video_info`` or ``presentation_info``).

This signature lets us decide whether the C++ binding needs a
``remove_incoming_video(old_endpoint)`` + ``add_incoming_video(new_endpoint, sources)``
re-subscription, even when the participant's ``video_camera`` /
``screen_sharing`` boolean flag has not flipped.

Why this is needed
------------------
Telegram's SFU re-issues a new ``endpoint`` for the same participant in
several normal scenarios:
  * camera off -> on toggled faster than the previous off propagated;
  * SDP renegotiation after a network handover (mobile -> WiFi);
  * simulcast layer reconfiguration (different ssrc-groups for the same
    encoder) without the participant ever logically dropping the camera.

The previous implementation only triggered ``add_incoming_video`` on a
boolean ``False -> True`` transition or on the very first appearance of
a user_id in the cache.  Anything else was a silent no-op -> the
recorder/listener received zero further frames for that user even though
audio kept arriving and the participant kept appearing in the live
participant list.
"""

from __future__ import annotations

from typing import Any, Optional, Tuple


def video_signature(info: Optional[Any]) -> Optional[Tuple[str, Tuple]]:
    """
    Return a hashable, comparison-stable signature for a video_info or
    presentation_info payload, or ``None`` if the participant has no
    such stream.

    Stable structure:
        (endpoint_str, ((semantics_str, (sorted_ssrc, ...)), ...))

    Sorted-by-semantics+ssrcs so equivalent payloads compare equal even
    if the upstream library reorders the ssrc-groups list across polls.
    """
    if info is None:
        return None
    endpoint = getattr(info, 'endpoint', '') or ''
    sources = getattr(info, 'sources', None) or []
    groups = []
    for sg in sources:
        sem = str(getattr(sg, 'semantics', '') or '')
        ssrcs_attr = getattr(sg, 'sources', None) or []
        try:
            ssrcs = tuple(sorted(int(s) & 0xFFFFFFFF for s in ssrcs_attr))
        except (TypeError, ValueError):
            # Defensive: if an ssrc isn't coercible, fall back to its
            # string repr so we still produce a deterministic signature.
            ssrcs = tuple(sorted(str(s) for s in ssrcs_attr))
        groups.append((sem, ssrcs))
    groups.sort()
    return (str(endpoint), tuple(groups))
