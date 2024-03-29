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

import * as JSONBig from 'json-bigint-native';

import type {
  AsyncStatusResponse,
  MsqTaskPayloadResponse,
  MsqTaskReportResponse,
} from '../druid-models';
import { Api } from '../singletons';

import { downloadFile } from './download';

export interface QueryDetailArchive {
  id: string;
  detailArchiveVersion: number;
  status?: any;
  reports?: MsqTaskReportResponse;
  payload?: MsqTaskPayloadResponse;
  statementsStatus?: AsyncStatusResponse;
  serverStatus?: any;
}

export async function downloadQueryDetailArchive(id: string) {
  const encodedId = Api.encodePath(id);
  const profile: QueryDetailArchive = {
    id,
    detailArchiveVersion: 2,
  };

  try {
    profile.status = (await Api.instance.get(`/druid/indexer/v1/task/${encodedId}/status`)).data;
  } catch {}

  try {
    profile.reports = (await Api.instance.get(`/druid/indexer/v1/task/${encodedId}/reports`)).data;
  } catch {}

  try {
    profile.payload = (await Api.instance.get(`/druid/indexer/v1/task/${encodedId}`)).data;
  } catch {}

  try {
    profile.statementsStatus = (
      await Api.instance.get(`/druid/v2/sql/statements/${encodedId}`)
    ).data;
  } catch {}

  try {
    profile.serverStatus = (await Api.instance.get(`/status`)).data;
  } catch {}

  downloadFile(JSONBig.stringify(profile, undefined, 2), 'json', `query_detail_${id}.archive.json`);
}
