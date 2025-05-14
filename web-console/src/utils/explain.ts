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

import { SqlQuery, SqlSetStatement } from 'druid-query-toolkit';

export function wrapInExplainIfNeeded(query: string): string {
  try {
    return wrapInExplainAsParsedIfNeeded(query);
  } catch {
    return wrapInExplainAsStringIfNeeded(query);
  }
}

export function wrapInExplainAsParsedIfNeeded(query: string): string {
  const parsed = SqlQuery.parse(query);
  if (parsed.explain) return query;
  return parsed.makeExplain().toString();
}

export function wrapInExplainAsStringIfNeeded(query: string): string {
  const [setPart, queryPart] = SqlSetStatement.partitionSetStatements(query, true);
  if (/^\s*EXPLAIN\sPLAN\sFOR/im.test(queryPart)) return query;

  // Only replace the first occurrence
  return setPart + queryPart.replace(/REPLACE|INSERT|SELECT|WITH/i, 'EXPLAIN PLAN FOR\n$&');
}
