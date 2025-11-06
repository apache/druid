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

import { Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { SqlQuery } from 'druid-query-toolkit';
import { L, SqlFunction } from 'druid-query-toolkit';

import { AppToaster } from '../../../singletons';
import { getMaxTimeForTable } from '../utils';

const tablesForWhichWeCouldNotDetermineMaxTime = new Set<string>();

export async function rewriteMaxDataTime(
  query: SqlQuery,
): Promise<{ query: SqlQuery; maxTime?: Date }> {
  if (!query.containsFunction('MAX_DATA_TIME')) return { query };

  const tableName = query.getFirstTableName();
  if (!tableName) throw new Error(`something went wrong - unable to find table name in query`);

  let maxTime: Date;
  try {
    maxTime = await getMaxTimeForTable(tableName);
  } catch (error) {
    if (!tablesForWhichWeCouldNotDetermineMaxTime.has(tableName)) {
      tablesForWhichWeCouldNotDetermineMaxTime.add(tableName);
      AppToaster.show({
        icon: IconNames.ERROR,
        intent: Intent.DANGER,
        timeout: 120000,
        message: `Could not determine max data time for ${tableName}: ${error.message}. Using current time instead.`,
      });
    }
    maxTime = new Date();
  }

  const adjustedMaxTime = new Date(maxTime.valueOf() + 1); // Add 1ms to the maxTime date to allow filters like `"__time" < {maxTime}" to capture the last event which might also be the only event

  return {
    query: query.walk(ex =>
      ex instanceof SqlFunction && ex.getEffectiveFunctionName() === 'MAX_DATA_TIME'
        ? L(adjustedMaxTime)
        : ex,
    ) as SqlQuery,
    maxTime: adjustedMaxTime,
  };
}
