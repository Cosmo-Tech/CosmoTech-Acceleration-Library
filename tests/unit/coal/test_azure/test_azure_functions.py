# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import json
import http
import pytest
from unittest.mock import MagicMock, patch, call

import azure.functions as func

from cosmotech.coal.azure.functions import generate_main


class TestFunctionsFunctions:
    """Tests for top-level functions in the functions module."""

    def test_generate_main_success(self):
        """Test the generate_main function with successful execution."""

        # Arrange
        # Mock the apply_update function
        def mock_apply_update(content, scenario_data):
            return {"updated": True, "data": content}

        # Mock the HttpRequest
        mock_req = MagicMock(spec=func.HttpRequest)
        mock_req.params = {
            "organization-id": "test-org",
            "workspace-id": "test-workspace",
            "scenario-id": "test-scenario",
        }
        mock_req.headers = {"authorization": "Bearer test-token"}

        # Mock the download_runner_data function
        mock_download_result = {
            "datasets": {"dataset1": {"data": "value1"}},
            "parameters": {"param1": "value1"},
            "runner_data": {"runner_info": "test"},
        }

        with patch("cosmotech.coal.azure.functions.download_runner_data", return_value=mock_download_result), patch(
            "cosmotech.coal.azure.functions.func.HttpResponse"
        ) as mock_http_response:
            # Create a mock HttpResponse
            mock_response = MagicMock()
            mock_http_response.return_value = mock_response

            # Act
            main_func = generate_main(mock_apply_update)
            result = main_func(mock_req)

            # Assert
            # Verify that download_runner_data was called with the correct parameters
            expected_content = {
                "datasets": {"dataset1": {"data": "value1"}},
                "parameters": {"param1": "value1"},
            }
            expected_updated_content = {"updated": True, "data": expected_content}

            # Verify that HttpResponse was called with the correct parameters
            mock_http_response.assert_called_once()
            call_args = mock_http_response.call_args[1]
            assert "body" in call_args
            assert json.loads(call_args["body"]) == expected_updated_content
            assert call_args["headers"] == {"Content-Type": "application/json"}

            # Verify the result
            assert result == mock_response

    def test_generate_main_missing_parameters(self):
        """Test the generate_main function with missing parameters."""

        # Arrange
        # Mock the apply_update function
        def mock_apply_update(content, scenario_data):
            return {"updated": True, "data": content}

        # Create test cases for different missing parameters
        test_cases = [
            # Missing organization-id
            {
                "params": {"workspace-id": "test-workspace", "scenario-id": "test-scenario"},
                "expected_status": http.HTTPStatus.BAD_REQUEST,
            },
            # Missing workspace-id
            {
                "params": {"organization-id": "test-org", "scenario-id": "test-scenario"},
                "expected_status": http.HTTPStatus.BAD_REQUEST,
            },
            # Missing scenario-id
            {
                "params": {"organization-id": "test-org", "workspace-id": "test-workspace"},
                "expected_status": http.HTTPStatus.BAD_REQUEST,
            },
        ]

        for test_case in test_cases:
            # Mock the HttpRequest
            mock_req = MagicMock(spec=func.HttpRequest)
            mock_req.params = test_case["params"]
            mock_req.headers = {"authorization": "Bearer test-token"}

            with patch("cosmotech.coal.azure.functions.func.HttpResponse") as mock_http_response:
                # Create a mock HttpResponse
                mock_response = MagicMock()
                mock_http_response.return_value = mock_response

                # Act
                main_func = generate_main(mock_apply_update)
                result = main_func(mock_req)

                # Assert
                # Verify that HttpResponse was called with the correct status code
                mock_http_response.assert_called_once()
                assert mock_http_response.call_args[1]["status_code"] == test_case["expected_status"]

                # Verify the result
                assert result == mock_response

    def test_generate_main_with_exception(self):
        """Test the generate_main function when an exception is thrown."""

        # Arrange
        # Mock the apply_update function
        def mock_apply_update(content, scenario_data):
            raise ValueError("Test error")

        # Mock the HttpRequest
        mock_req = MagicMock(spec=func.HttpRequest)
        mock_req.params = {
            "organization-id": "test-org",
            "workspace-id": "test-workspace",
            "scenario-id": "test-scenario",
        }
        mock_req.headers = {"authorization": "Bearer test-token"}

        # Mock the download_runner_data function
        mock_download_result = {
            "datasets": {"dataset1": {"data": "value1"}},
            "parameters": {"param1": "value1"},
            "runner_data": {"runner_info": "test"},
        }

        with patch("cosmotech.coal.azure.functions.download_runner_data", return_value=mock_download_result), patch(
            "cosmotech.coal.azure.functions.func.HttpResponse"
        ) as mock_http_response, patch(
            "cosmotech.coal.azure.functions.traceback.format_exc", return_value="test traceback"
        ):
            # Create a mock HttpResponse
            mock_response = MagicMock()
            mock_http_response.return_value = mock_response

            # Act
            main_func = generate_main(mock_apply_update)
            result = main_func(mock_req)

            # Assert
            # Verify that HttpResponse was called with the correct parameters
            mock_http_response.assert_called_once()
            call_args = mock_http_response.call_args[1]
            assert call_args["status_code"] == http.HTTPStatus.INTERNAL_SERVER_ERROR
            assert "body" in call_args
            response_body = json.loads(call_args["body"])
            assert response_body["error"] == "Test error"
            assert response_body["type"] == "ValueError"
            assert response_body["trace"] == "test traceback"
            assert call_args["headers"] == {"Content-Type": "application/json"}

            # Verify the result
            assert result == mock_response

    def test_generate_main_without_auth_token(self):
        """Test the generate_main function without an authorization token."""

        # Arrange
        # Mock the apply_update function
        def mock_apply_update(content, scenario_data):
            return {"updated": True, "data": content}

        # Mock the HttpRequest
        mock_req = MagicMock(spec=func.HttpRequest)
        mock_req.params = {
            "organization-id": "test-org",
            "workspace-id": "test-workspace",
            "scenario-id": "test-scenario",
        }
        mock_req.headers = {}  # No authorization header

        # Mock the download_runner_data function
        mock_download_result = {
            "datasets": {"dataset1": {"data": "value1"}},
            "parameters": {"param1": "value1"},
            "runner_data": {"runner_info": "test"},
        }

        with patch("cosmotech.coal.azure.functions.download_runner_data", return_value=mock_download_result), patch(
            "cosmotech.coal.azure.functions.func.HttpResponse"
        ) as mock_http_response:
            # Create a mock HttpResponse
            mock_response = MagicMock()
            mock_http_response.return_value = mock_response

            # Act
            main_func = generate_main(mock_apply_update)
            result = main_func(mock_req)

            # Assert
            # Verify that download_runner_data was called with the correct parameters
            expected_content = {
                "datasets": {"dataset1": {"data": "value1"}},
                "parameters": {"param1": "value1"},
            }
            expected_updated_content = {"updated": True, "data": expected_content}

            # Verify that HttpResponse was called with the correct parameters
            mock_http_response.assert_called_once()
            call_args = mock_http_response.call_args[1]
            assert "body" in call_args
            assert json.loads(call_args["body"]) == expected_updated_content
            assert call_args["headers"] == {"Content-Type": "application/json"}

            # Verify the result
            assert result == mock_response

    def test_generate_main_with_parallel_false(self):
        """Test the generate_main function with parallel=False."""

        # Arrange
        # Mock the apply_update function
        def mock_apply_update(content, scenario_data):
            return {"updated": True, "data": content}

        # Mock the HttpRequest
        mock_req = MagicMock(spec=func.HttpRequest)
        mock_req.params = {
            "organization-id": "test-org",
            "workspace-id": "test-workspace",
            "scenario-id": "test-scenario",
        }
        mock_req.headers = {"authorization": "Bearer test-token"}

        # Mock the download_runner_data function
        mock_download_result = {
            "datasets": {"dataset1": {"data": "value1"}},
            "parameters": {"param1": "value1"},
            "runner_data": {"runner_info": "test"},
        }

        with patch(
            "cosmotech.coal.azure.functions.download_runner_data", return_value=mock_download_result
        ) as mock_download, patch("cosmotech.coal.azure.functions.func.HttpResponse") as mock_http_response:
            # Create a mock HttpResponse
            mock_response = MagicMock()
            mock_http_response.return_value = mock_response

            # Act
            main_func = generate_main(mock_apply_update, parallel=False)
            result = main_func(mock_req)

            # Assert
            # Verify that download_runner_data was called with parallel=False
            mock_download.assert_called_once()
            assert mock_download.call_args[1]["parallel"] is False

            # Verify the result
            assert result == mock_response
