"""Unit tests for the S3 writer module."""

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from src.lambda.s3_writer import generate_s3_key, write_pokemon_data


class TestGenerateS3Key:
    """Tests for the generate_s3_key function."""

    def test_standard_date_and_name(self):
        result = generate_s3_key("2024-01-15", "bulbasaur")
        assert result == "bronze/2024/01/15/bulbasaur.json"

    def test_end_of_year_date(self):
        result = generate_s3_key("2023-12-31", "pikachu")
        assert result == "bronze/2023/12/31/pikachu.json"

    def test_single_digit_month_and_day_preserved(self):
        result = generate_s3_key("2024-03-05", "charmander")
        assert result == "bronze/2024/03/05/charmander.json"

    def test_hyphenated_pokemon_name(self):
        result = generate_s3_key("2024-06-10", "mr-mime")
        assert result == "bronze/2024/06/10/mr-mime.json"

    def test_key_starts_with_bronze_prefix(self):
        result = generate_s3_key("2024-01-01", "eevee")
        assert result.startswith("bronze/")

    def test_key_ends_with_json_extension(self):
        result = generate_s3_key("2024-01-01", "eevee")
        assert result.endswith(".json")


class TestWritePokemonData:
    """Tests for the write_pokemon_data function."""

    @patch("src.lambda.s3_writer.boto3.client")
    def test_successful_write(self, mock_boto_client):
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        data = {"id": 1, "name": "bulbasaur", "height": 7}
        result = write_pokemon_data("my-bucket", "2024-01-15", "bulbasaur", data)

        assert result is True
        mock_s3.put_object.assert_called_once_with(
            Bucket="my-bucket",
            Key="bronze/2024/01/15/bulbasaur.json",
            Body='{"id": 1, "name": "bulbasaur", "height": 7}',
            ContentType="application/json",
        )

    @patch("src.lambda.s3_writer.boto3.client")
    def test_retry_on_first_failure_then_success(self, mock_boto_client):
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        error_response = {"Error": {"Code": "InternalError", "Message": "Service error"}}
        mock_s3.put_object.side_effect = [
            ClientError(error_response, "PutObject"),
            None,  # Success on retry
        ]

        data = {"id": 25, "name": "pikachu"}
        result = write_pokemon_data("my-bucket", "2024-03-01", "pikachu", data)

        assert result is True
        assert mock_s3.put_object.call_count == 2

    @patch("src.lambda.s3_writer.boto3.client")
    def test_returns_false_after_two_failures(self, mock_boto_client):
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        error_response = {"Error": {"Code": "InternalError", "Message": "Service error"}}
        mock_s3.put_object.side_effect = ClientError(error_response, "PutObject")

        data = {"id": 4, "name": "charmander"}
        result = write_pokemon_data("my-bucket", "2024-02-20", "charmander", data)

        assert result is False
        assert mock_s3.put_object.call_count == 2
