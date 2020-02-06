# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from typing import Dict, Tuple
import os

from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import TaskInstance
from google.cloud.logging import Client


class StackdriveTaskHandler(FileTaskHandler, LoggingMixin):
    """
    StackdriveTaskHandler is a python log handler that handles and reads
    task instance logs. It extends airflow FileTaskHandler and
    uploads to and reads from stackdrive logging API. Upon log reading
    failure, it reads from host machine's local disk.
    """
    def __init__(self, base_log_folder, filename_template):
        super().__init__(base_log_folder, filename_template)
        self._client = Client()
        self.log_relative_path = ''
        self.upload_on_close = True
        self._logger = self._client.logger('Airflow DAG Log')



    def set_context(self, ti: TaskInstance):
        super().set_context(ti)
        # Log relative path is used to construct local and remote
        # log path to upload log files into GCS and read from the
        # remote location.
        self.log_lables = {'dag_id':ti.dag_id,
                           'task_id':ti.task_id,
                           'execution_date':ti.execution_date.isoformat(),
                           'try_number':try_number}
        self.upload_on_close = not ti.raw

    def close(self):
        """
        Close and upload local log file to remote storage GCS.
        """
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return
        super().close()

        if not self.upload_on_close:
            return



        local_loc = os.path.join(self.local_base, self.log_relative_path)

        if os.path.exists(local_loc):
            # read log and remove old logs to get just the latest additions
            with open(local_loc, 'r') as logfile:
                log = logfile.read()
                self._logger.log_struct(lables=self.log_lables, text_payload=log)


        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def _read(self, ti, try_number, metadata=None) -> Tuple(str, Dict):
        """
        Read logs of given task instance and try_number from GCS.
        If failed, read the log from task instance host machine.

        :param ti: task instance object
        :param try_number: task instance try_number to read logs from
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
        :returns:
        """
        # Explicitly getting log relative path is necessary as the given
        # task instance might be different than task instance passed in
        # in set_context method.
        log_relative_path = self._render_filename(ti, try_number)
        remote_loc = os.path.join(self.remote_base, log_relative_path)

        try:
            remote_log = self.gcs_read(remote_loc)
            log = '*** Reading remote log from {}.\n{}\n'.format(
                remote_loc, remote_log)
            return log, {'end_of_log': True}
        except Exception as e:  # pylint: disable=broad-except
            log = '*** Unable to read remote log from {}\n*** {}\n\n'.format(
                remote_loc, str(e))
            self.log.error(log)
            local_log, metadata = super()._read(ti, try_number)
            log += local_log
            return log, metadata
