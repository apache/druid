/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React from 'react';

import type { InputSource } from '../../../druid-models';

export interface InputSourceInfoProps {
  inputSource: Partial<InputSource>;
}

export const InputSourceInfo = React.memo(function InputSourceInfo(props: InputSourceInfoProps) {
  const { inputSource } = props;

  switch (inputSource.type) {
    case 'http':
      return (
        <>
          <p>Load data accessible through HTTP(s).</p>
          <p>
            Data must be in text, orc, or parquet format and the HTTP(s) endpoint must be reachable
            by every Druid process in the cluster.
          </p>
        </>
      );

    case 'local':
      return (
        <>
          <p>
            <em>Recommended only in single server deployments.</em>
          </p>
          <p>Load data directly from a local file.</p>
          <p>
            Files must be in text, orc, or parquet format and must be accessible to all the Druid
            processes in the cluster.
          </p>
        </>
      );

    case 'inline':
      return (
        <p>
          Ingest a small amount of data by typing it in directly (or pasting from the clipboard).
        </p>
      );

    case 's3':
      return <p>Load text based, avro, orc, or parquet data from Amazon S3.</p>;

    case 'azureStorage':
      return <p>Load text based, avro, orc, or parquet data from Azure.</p>;

    case 'google':
      return <p>Load text based, avro, orc, or parquet data from the Google Blobstore.</p>;

    case 'delta':
      return <p>Load data from Delta Lake.</p>;

    case 'hdfs':
      return <p>Load text based, avro, orc, or parquet data from HDFS.</p>;

    default:
      return null;
  }
});
