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

import csv
import sys
import json
import time
import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from decimal import Decimal
from tempfile import NamedTemporaryFile

PY3 = sys.version_info[0] == 3


class PostgresToS3Operator(BaseOperator):
    """
    Copy data from Postgres to S3 in TSV format.
    """
    template_fields = ('table', 's3_bucket', 's3_key')
    template_ext = ('.sql', )
    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(self,
                 table,
                 s3_bucket,
                 s3_key,
                 data_dir,
                 postgres_conn_id='postgres_dev_db',
                 s3_conn_id='aws_s3_default',
                 *args,
                 **kwargs):
        """
        :param postgres_conn_id: Reference to a specific Postgres hook.
        :type postgres_conn_id: string
        :param table: The name of a table to copy
        :type table: string
        :param s3_conn_id
        :type s3_conn_id: string
        :param s3_bucket
        :type s3_bucket: string
        :param s3_key
        :type s3_key: string
        """
        super(PostgresToS3Operator, self).__init__(*args, **kwargs)

        self.postgres_conn_id = postgres_conn_id
        self.table = table

        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.data_dir = data_dir
        self.s3_conn_id = s3_conn_id
        self.parameters = ''
        self.filename = table + '_{}.tsv'
        self.replace = True

    def execute(self, context):
        file_no = 0
        filename = self.filename.format(file_no)
        self.log.info("running Postgres query: {table}".format(table=self.table))

        io = open(self.data_dir + filename, 'w')
        cursor = self._query_postgres(io)
        io.close()

        self.log.info("uploading to S3: {file}".format(file=filename))
        self._upload_to_s3([filename])

    def _query_postgres(self, io):
        """
        Queries Postgres and returns a cursor to the results.
        """
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = postgres.get_conn()
        cursor = conn.cursor()
        cursor.copy_to(io, self.table, null='')
        # cursor.execute(self.sql, self.parameters)
        return cursor

    def _write_local_data_files(self, cursor):
        """
        Takes a cursor, and writes results to a local file.
        :return: A dictionary where keys are filenames to be used as object
            names in GCS, and values are file handles to local files that
            contain the data for the GCS objects.
        """
        file_no = 0
        tmp_file_handle = NamedTemporaryFile(delete=True)
        tmp_file_handles = {self.filename.format(file_no): tmp_file_handle}

        for row in cursor:
            self.log.info(row)
            if PY3:
                row = row.encode('utf-8')
            tmp_file_handle.write(row)

            # Append newline to make dumps BigQuery compatible.
            tmp_file_handle.write(b'\n')

            # Stop if the file exceeds the file size limit.
            if tmp_file_handle.tell() >= self.approx_max_file_size_bytes:
                file_no += 1
                tmp_file_handle = NamedTemporaryFile(delete=True)
                tmp_file_handles[self.filename.format(file_no)] = tmp_file_handle

        return tmp_file_handles

    def _upload_to_s3(self, files_to_upload):
        """
        Upload all of the files to S3
        """
        s3_hook = S3Hook(self.s3_conn_id)
        for file in files_to_upload:
            dest_key = self.s3_key
            self.log.info("Saving file to %s in S3", dest_key)
            self.log.info(file)
            s3_hook.load_file(
                filename=self.data_dir + file,
                key=dest_key,
                bucket_name=self.s3_bucket,
                replace=self.replace)
