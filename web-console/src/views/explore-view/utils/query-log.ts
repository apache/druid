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

interface QueryLogEntry {
  time: Date;
  sqlQuery: string;
}

const MAX_QUERIES_TO_LOG = 10;

export class QueryLog {
  private readonly queryLog: QueryLogEntry[] = [];

  public length(): number {
    return this.queryLog.length;
  }

  public addQuery(sqlQuery: string): void {
    const { queryLog } = this;
    queryLog.unshift({ time: new Date(), sqlQuery });
    while (queryLog.length > MAX_QUERIES_TO_LOG) queryLog.pop();
  }

  public getLastQuery(): string | undefined {
    return this.queryLog[0]?.sqlQuery;
  }

  public getFormatted(): string {
    return this.queryLog
      .map(({ time, sqlQuery }) => `At ${time.toISOString()} ran query:\n\n${sqlQuery}`)
      .join('\n\n-----------------------------------------------------\n\n');
  }
}
