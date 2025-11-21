# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pytest

from cosmotech.coal.store.output.channel_interface import ChannelInterface


class TestChannelInterface:
    """Tests for the ChannelInterface base class."""

    def test_send_not_implemented(self):
        """Test that send method raises NotImplementedError."""
        # Arrange
        channel = ChannelInterface()

        # Act & Assert
        with pytest.raises(NotImplementedError):
            channel.send()

    def test_send_with_filter_not_implemented(self):
        """Test that send method with filter raises NotImplementedError."""
        # Arrange
        channel = ChannelInterface()

        # Act & Assert
        with pytest.raises(NotImplementedError):
            channel.send(filter=["table1", "table2"])

    def test_delete_not_implemented(self):
        """Test that delete method raises NotImplementedError."""
        # Arrange
        channel = ChannelInterface()

        # Act & Assert
        with pytest.raises(NotImplementedError):
            channel.delete()

    def test_is_available_no_required_keys(self):
        """Test is_available when no required keys are defined."""
        # Arrange
        channel = ChannelInterface()
        channel.required_keys = {}
        channel.configuration = {}

        # Act
        result = channel.is_available()

        # Assert
        assert result is True

    def test_is_available_with_required_keys_present(self):
        """Test is_available when all required keys are present."""
        # Arrange
        channel = ChannelInterface()
        channel.required_keys = {"section1": ["key1", "key2"], "section2": ["key3"]}
        channel.configuration = {"section1": {"key1": "value1", "key2": "value2"}, "section2": {"key3": "value3"}}

        # Act
        result = channel.is_available()

        # Assert
        assert result is True

    def test_is_available_with_missing_keys(self):
        """Test is_available when required keys are missing."""
        # Arrange
        channel = ChannelInterface()
        channel.required_keys = {"section1": ["key1", "key2"], "section2": ["key3"]}
        channel.configuration = {"section1": {"key1": "value1"}, "section2": {"key3": "value3"}}  # missing key2

        # Act
        result = channel.is_available()

        # Assert
        assert result is False

    def test_is_available_with_missing_section(self):
        """Test is_available when a required section is missing."""
        # Arrange
        channel = ChannelInterface()
        channel.required_keys = {"section1": ["key1", "key2"], "section2": ["key3"]}
        channel.configuration = {
            "section1": {"key1": "value1", "key2": "value2"}
            # section2 is missing
        }

        # Act & Assert
        # The is_available method will raise KeyError when section is missing
        # This is the actual behavior of the current implementation
        # Act
        result = channel.is_available()

        # Assert
        assert result is False

    def test_is_available_with_extra_keys(self):
        """Test is_available when extra keys are present (should still be available)."""
        # Arrange
        channel = ChannelInterface()
        channel.required_keys = {"section1": ["key1"]}
        channel.configuration = {"section1": {"key1": "value1", "key2": "value2", "key3": "value3"}}

        # Act
        result = channel.is_available()

        # Assert
        assert result is True

    def test_is_available_empty_required_keys_in_section(self):
        """Test is_available when a section has no required keys."""
        # Arrange
        channel = ChannelInterface()
        channel.required_keys = {"section1": []}
        channel.configuration = {"section1": {"key1": "value1"}}

        # Act
        result = channel.is_available()

        # Assert
        assert result is True

    def test_requirement_string_default(self):
        """Test that requirement_string has a default value."""
        # Arrange
        channel = ChannelInterface()

        # Act & Assert
        assert hasattr(channel, "requirement_string")
        assert isinstance(channel.requirement_string, str)
