# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pytest

from cosmotech.coal.store.output.channel_interface import ChannelInterface


class TestChannelInterface:
    """Tests for ChannelInterface base class."""

    def test_send_not_implemented(self):
        """Test that send raises NotImplementedError."""
        # Arrange
        interface = ChannelInterface()

        # Act & Assert
        with pytest.raises(NotImplementedError):
            interface.send()

    def test_send_with_filter_not_implemented(self):
        """Test that send with filter raises NotImplementedError."""
        # Arrange
        interface = ChannelInterface()

        # Act & Assert
        with pytest.raises(NotImplementedError):
            interface.send(filter=["table1", "table2"])

    def test_delete_not_implemented(self):
        """Test that delete raises NotImplementedError."""
        # Arrange
        interface = ChannelInterface()

        # Act & Assert
        with pytest.raises(NotImplementedError):
            interface.delete()

    def test_is_available_not_implemented(self):
        """Test that is_available raises NotImplementedError."""
        # Arrange
        interface = ChannelInterface()

        # Act & Assert
        with pytest.raises(NotImplementedError):
            interface.is_available()

    def test_requirement_string_exists(self):
        """Test that requirement_string attribute exists."""
        # Arrange
        interface = ChannelInterface()

        # Act & Assert
        assert hasattr(interface, "requirement_string")
        assert isinstance(interface.requirement_string, str)
