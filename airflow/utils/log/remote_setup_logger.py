import re
from abc import ABC, abstractclassmethod
from typing import Any, Dict
from airflow.exceptions import AirflowException, AirflowConfigException
from airflow.configuration import AirflowConfigParser, _UNSET

class AirflowValidationException(AirflowException):
    pass


class RemoteSetupLogger(object):
    """
    [remote_logging]
    s3_enabled = 
    s3_base_log_folder = 
    s3_remote_log_conn_id =
    gcs_enabled =
    gcs_base_log_folder = 
    wasb_enabled = 
    wasb_base_log_folder = 
    elasticsearch_enabled = 
    elasticsearch_log_id_template = ,
    elasticsearch_end_of_log_mark = ,
    elasticsearch_host =,
    elasticsearch_write_stdout = ,
    elasticsearch_json_format = ,
    elasticsearch_json_fields
    """
    logging_backends = {
        "elastic": {},
        "gcs": {},
        "s3": {},
        "wasb": {}
    }

    def __init__(self):
        self.remote_logging: bool = False

    def setup_logging(self, logging_config: Dict[str, Any], config: AirflowConfigParser):
        self.remote_logging = config.getboolean('logging', 'remote_logging')


class AirflowConfigValidator(ABC):
    @abstractclassmethod
    def validate(self, config: AirflowConfigParser):
        raise NotImplementedError()


class S3Validator(AirflowConfigValidator):
    def __init__(self):
        self.section = 'remote_logging'
        self.remote_log_folder = 's3_remote_log_folder'
        self.s3_enabled = 's3_enabled'
        self.s3_encrypt_logs = 's3_encrypt_logs'
        self.s3_remote_log_conn_id = 's3_remote_log_conn_id'

    def validate(self, config: AirflowConfigParser):
        is_enabled = config.getboolean_with_default(self.section, self.s3_enabled, False)
        if is_enabled:
            try:
                self._validate_remote_log_folder(config)
                config.get(self.section, self.s3_remote_log_conn_id)
                encrypt_logs = config.getboolean_with_default(self.section, self.s3_encrypt_logs, False)
            except ValueError as err:
                raise AirflowValidationException(err)

    def _validate_remote_log_folder(self, config: AirflowConfigParser):
        try:
            remote_log_folder = config.get(self.section, self.remote_log_folder)
            if not re.match(r'^(s3://).*', remote_log_folder):
                raise AirflowValidationException(f'{self.remote_log_folder}: {remote_log_folder} is not valid S3 bucket name. Valid name should match regex: ^(s3://).*')
        except AirflowConfigException as err:
            raise AirflowValidationException(err)



# if REMOTE_LOGGING:

#     ELASTICSEARCH_HOST: str = conf.get('elasticsearch', 'HOST')

#     # Storage bucket URL for remote logging
#     # S3 buckets should start with "s3://"
#     # GCS buckets should start with "gs://"
#     # WASB buckets should start with "wasb"
#     # just to help Airflow select correct handler
#     REMOTE_BASE_LOG_FOLDER: str = conf.get('logging', 'REMOTE_BASE_LOG_FOLDER')

#     if REMOTE_BASE_LOG_FOLDER.startswith('s3://'):
#         S3_REMOTE_HANDLERS: Dict[str, Dict[str, str]] = {
#             'task': {
#                 'class': 'airflow.utils.log.s3_task_handler.S3TaskHandler',
#                 'formatter': 'airflow',
#                 'base_log_folder': str(os.path.expanduser(BASE_LOG_FOLDER)),
#                 's3_log_folder': REMOTE_BASE_LOG_FOLDER,
#                 'filename_template': FILENAME_TEMPLATE,
#             },
#         }

#         DEFAULT_LOGGING_CONFIG['handlers'].update(S3_REMOTE_HANDLERS)
#     elif REMOTE_BASE_LOG_FOLDER.startswith('gs://'):
#         GCS_REMOTE_HANDLERS: Dict[str, Dict[str, str]] = {
#             'task': {
#                 'class': 'airflow.utils.log.gcs_task_handler.GCSTaskHandler',
#                 'formatter': 'airflow',
#                 'base_log_folder': str(os.path.expanduser(BASE_LOG_FOLDER)),
#                 'gcs_log_folder': REMOTE_BASE_LOG_FOLDER,
#                 'filename_template': FILENAME_TEMPLATE,
#             },
#         }

#         DEFAULT_LOGGING_CONFIG['handlers'].update(GCS_REMOTE_HANDLERS)
#     elif REMOTE_BASE_LOG_FOLDER.startswith('wasb'):
#         WASB_REMOTE_HANDLERS: Dict[str, Dict[str, Union[str, bool]]] = {
#             'task': {
#                 'class': 'airflow.utils.log.wasb_task_handler.WasbTaskHandler',
#                 'formatter': 'airflow',
#                 'base_log_folder': str(os.path.expanduser(BASE_LOG_FOLDER)),
#                 'wasb_log_folder': REMOTE_BASE_LOG_FOLDER,
#                 'wasb_container': 'airflow-logs',
#                 'filename_template': FILENAME_TEMPLATE,
#                 'delete_local_copy': False,
#             },
#         }

#         DEFAULT_LOGGING_CONFIG['handlers'].update(WASB_REMOTE_HANDLERS)
#     elif ELASTICSEARCH_HOST:
#         ELASTICSEARCH_LOG_ID_TEMPLATE: str = conf.get('elasticsearch', 'LOG_ID_TEMPLATE')
#         ELASTICSEARCH_END_OF_LOG_MARK: str = conf.get('elasticsearch', 'END_OF_LOG_MARK')
#         ELASTICSEARCH_WRITE_STDOUT: bool = conf.getboolean('elasticsearch', 'WRITE_STDOUT')
#         ELASTICSEARCH_JSON_FORMAT: bool = conf.getboolean('elasticsearch', 'JSON_FORMAT')
#         ELASTICSEARCH_JSON_FIELDS: str = conf.get('elasticsearch', 'JSON_FIELDS')

#         ELASTIC_REMOTE_HANDLERS: Dict[str, Dict[str, Union[str, bool]]] = {
#             'task': {
#                 'class': 'airflow.utils.log.es_task_handler.ElasticsearchTaskHandler',
#                 'formatter': 'airflow',
#                 'base_log_folder': str(os.path.expanduser(BASE_LOG_FOLDER)),
#                 'log_id_template': ELASTICSEARCH_LOG_ID_TEMPLATE,
#                 'filename_template': FILENAME_TEMPLATE,
#                 'end_of_log_mark': ELASTICSEARCH_END_OF_LOG_MARK,
#                 'host': ELASTICSEARCH_HOST,
#                 'write_stdout': ELASTICSEARCH_WRITE_STDOUT,
#                 'json_format': ELASTICSEARCH_JSON_FORMAT,
#                 'json_fields': ELASTICSEARCH_JSON_FIELDS
#             },
#         }

#         DEFAULT_LOGGING_CONFIG['handlers'].update(ELASTIC_REMOTE_HANDLERS)
#     else:
#         raise AirflowException(
#             "Incorrect remote log configuration. Please check the configuration of option 'host' in "
#             "section 'elasticsearch' if you are using Elasticsearch. In the other case, "
#             "'remote_base_log_folder' option in 'core' section.")
