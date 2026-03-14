# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_seer

from importlib import reload
from pathlib import Path
from unittest import mock


def test_logger_creates_directory() -> None:
    """Test that the logger setup creates the logs directory if it does not exist."""
    # If it exists, remove it or pretend it doesn't
    with mock.patch.object(Path, "exists", return_value=False), mock.patch.object(Path, "mkdir") as mock_mkdir:
        import coreason_etl_seer.utils.logger as log_module

        reload(log_module)
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
