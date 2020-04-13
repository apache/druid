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

export interface QueryRecord {
  version: string;
  queryString: string;
  queryContext?: Record<string, any>;
}

export class QueryRecordUtil {
  static getHistoryVersion(): string {
    return new Date()
      .toISOString()
      .split('.')[0]
      .replace('T', ' ');
  }

  static addQueryToHistory(
    queryHistory: readonly QueryRecord[],
    queryString: string,
    queryContext: Record<string, any>,
  ): readonly QueryRecord[] {
    // Do not add to history if already the same as the last element in query and context
    if (
      queryHistory.length &&
      queryHistory[0].queryString === queryString &&
      JSON.stringify(queryHistory[0].queryContext) === JSON.stringify(queryContext)
    ) {
      return queryHistory;
    }

    return [
      {
        version: QueryRecordUtil.getHistoryVersion(),
        queryString,
        queryContext,
      } as QueryRecord,
    ]
      .concat(queryHistory)
      .slice(0, 10);
  }
}
