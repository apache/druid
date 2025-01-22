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

// micro-cache
import { sql, T } from 'druid-query-toolkit';

import { deepGet, queryDruidSql } from '../../../utils';

const MAX_TIME_TTL = 60000;
let lastMaxTimeTable: string | undefined;
let lastMaxTimeValue: Date | undefined;
let lastMaxTimeTimestamp = 0;

export async function getMaxTimeForTable(tableName: string): Promise<Date | undefined> {
  // micro-cache get
  if (
    lastMaxTimeTable === tableName &&
    lastMaxTimeValue &&
    Date.now() < lastMaxTimeTimestamp + MAX_TIME_TTL
  ) {
    return lastMaxTimeValue;
  }

  const d = await queryDruidSql({
    query: sql`SELECT MAX(__time) AS "maxTime" FROM ${T(tableName)}`,
  });

  const maxTime = new Date(deepGet(d, '0.maxTime'));
  if (isNaN(maxTime.valueOf())) return;

  // micro-cache set
  lastMaxTimeTable = tableName;
  lastMaxTimeValue = maxTime;
  lastMaxTimeTimestamp = Date.now();

  return maxTime;
}
