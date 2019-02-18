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

import axios from 'axios';
import { AxiosResponse } from 'axios';

export function getDruidErrorMessage(e: any) {
  const data: any = ((e.response || {}).data || {});
  return [data.error, data.errorMessage, data.errorClass].filter(Boolean).join(' / ') || e.message;
}

export async function queryDruidRune(runeQuery: Record<string, any>): Promise<any> {
  let runeResultResp: AxiosResponse<any>;
  try {
    runeResultResp = await axios.post("/druid/v2", runeQuery);
  } catch (e) {
    throw new Error(getDruidErrorMessage(e));
  }
  return runeResultResp.data;
}

export async function queryDruidSql(sqlQuery: Record<string, any>): Promise<any[]> {
  let sqlResultResp: AxiosResponse<any>;
  try {
    sqlResultResp = await axios.post("/druid/v2/sql", sqlQuery);
  } catch (e) {
    throw new Error(getDruidErrorMessage(e));
  }
  return sqlResultResp.data;
}
