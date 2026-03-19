# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_seer

"""Defines strict domain exceptions (fault events) for the SEER integration."""


class EpistemicSeerFaultEvent(Exception):
    """Base exception class representing a generic failure within the SEER client policy."""


class SeerRateLimitFaultEvent(EpistemicSeerFaultEvent):
    """Exception raised when the SEER API enforces a 429 Too Many Requests rate limit."""


class SeerGatewayFaultEvent(EpistemicSeerFaultEvent):
    """Exception raised when the SEER API encounters 50X server-side gateway issues."""


class SeerResourceNotFoundFaultEvent(EpistemicSeerFaultEvent):
    """Exception raised when a requested resource returns a 404 Not Found."""


class SeerAuthenticationFaultEvent(EpistemicSeerFaultEvent):
    """Exception raised when the SEER API rejects the provided API key (401 Unauthorized or 403 Forbidden)."""
