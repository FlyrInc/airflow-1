import unittest
import pytest
from airflow.utils.log.remote_setup_logger import S3Validator, AirflowValidationException
from airflow.configuration import AirflowConfigParser
from parameterized import parameterized


DEFAULT_CONFIG = '''
[remote_logging]
s3_enabled = true
s3_remote_log_folder = s3://my_test_bucket/airflow_logs
s3_encrypt_logs = false
s3_remote_log_conn_id = aws_airflow_logs
'''


INVALID_S3_FOLDER_CONFIG = '''
[remote_logging]
s3_enabled = true
s3_remote_log_folder = {}
s3_encrypt_logs = false
s3_remote_log_conn_id = aws_airflow_logs
'''

INVALID_S3_ENCRYPT_LOGS_CONFIG = '''
[remote_logging]
s3_enabled = true
s3_remote_log_folder = s3://my_test_bucket/airflow_logs
s3_encrypt_logs = {}
s3_remote_log_conn_id = aws_airflow_logs
'''

INVALID_S3_REMOTE_LOG_CONNECTION_ID_CONFIG = '''
[remote_logging]
s3_enabled = true
s3_remote_log_folder = s3://my_test_bucket/airflow_logs
s3_encrypt_logs = false
'''

class TestAirflowValidator(unittest.TestCase):

    def test_should_validate_s3_config_with_error(self):
        # Given
        validator = S3Validator()
        config = AirflowConfigParser()
        config.read_string(DEFAULT_CONFIG)
        # When - Then
        validator.validate(config)

    @parameterized.expand([
        ["my_test_bucket/airflow_logs"],
        ["s2://my_test_bucket/airflow_logs"],
        ["http://my_test_bucket/airflow_logs"]
    ])
    def test_should_raise_exception_when_s3_remote_log_folder_is_not_valid_s3_path(self, remote_log_foler):
        # Given
        parsed_config = INVALID_S3_FOLDER_CONFIG.replace('{}', remote_log_foler)
        validator = S3Validator()
        config = AirflowConfigParser()
        config.read_string(parsed_config)
        # When
        with pytest.raises(AirflowValidationException) as excinfo:
            validator.validate(config)
        # Then
        self.assertEqual(f's3_remote_log_folder: {remote_log_foler} is not valid S3 bucket name. Valid name should match regex: ^(s3://).*', str(excinfo.value))


    @parameterized.expand([
        ["y"],
        ["yes"],
        ["no"],
        ["-1"],
        ["10"]
    ])
    def test_should_raise_exception_when_s3_encrypt_logs_is_not_boolean(self, encrypt_logs):
        # Given
        parsed_config = INVALID_S3_ENCRYPT_LOGS_CONFIG.replace('{}', encrypt_logs)
        validator = S3Validator()
        config = AirflowConfigParser()
        config.read_string(parsed_config)
        expected_error_message = 'The value for configuration option "remote_logging:s3_encrypt_logs" is not a boolean (received "{}").'.format(encrypt_logs)
        # When
        with pytest.raises(AirflowValidationException) as excinfo:
            validator.validate(config)

        # Then
        self.assertEqual(expected_error_message, str(excinfo.value))

    def test_should_raise_exception_when_s3_remote_log_conn_id_is_not_set(self):
        # Given
        validator = S3Validator()
        config = AirflowConfigParser()
        config.read_string(INVALID_S3_REMOTE_LOG_CONNECTION_ID_CONFIG)
        expected_error_message = 'The value for configuration option "remote_logging:s3_remote_log_conn_id" is not a boolean (received "{}").'.format(encrypt_logs)
        # When
        with pytest.raises(AirflowValidationException) as excinfo:
            validator.validate(config)

        # Then
        self.assertEqual(expected_error_message, str(excinfo.value))
