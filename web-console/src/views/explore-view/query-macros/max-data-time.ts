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

import type { SqlQuery } from 'druid-query-toolkit';
import { L, SqlFunction } from 'druid-query-toolkit';

import { getMaxTimeForTable } from '../utils';

export async function rewriteMaxDataTime(query: SqlQuery) {
  if (!query.containsFunction('MAX_DATA_TIME')) return query;

  const tableName = query.getFirstTableName();
  if (!tableName) return query;

  const maxTime = await getMaxTimeForTable(tableName);
  if (!maxTime) return query;

  return query.walk(ex =>
    ex instanceof SqlFunction && ex.getEffectiveFunctionName() === 'MAX_DATA_TIME'
      ? L(new Date(maxTime.valueOf() + 1)) // Add 1ms to the maxTime date to allow filters like `"__time" < {maxTime}" to capture the last event which might also be the only event
      : ex,
  ) as SqlQuery;
}
