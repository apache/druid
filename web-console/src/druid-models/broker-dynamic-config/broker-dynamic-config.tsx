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

import type { Field } from '../../components';

export interface PerSegmentTimeoutConfig {
  perSegmentTimeoutMs: number;
  monitorOnly?: boolean;
}

export interface BrokerDynamicConfig {
  queryBlocklist?: QueryBlocklistRule[];
  queryContext?: Record<string, unknown>;
  perSegmentTimeoutConfig?: Record<string, PerSegmentTimeoutConfig>;
}

export interface QueryBlocklistRule {
  ruleName: string;
  dataSources?: string[];
  queryTypes?: string[];
  contextMatches?: Record<string, string>;
}

export const BROKER_DYNAMIC_CONFIG_FIELDS: Field<BrokerDynamicConfig>[] = [
  {
    name: 'queryContext',
    type: 'json',
    info: (
      <>
        Default query context values applied to all queries on this broker. These override static
        defaults from runtime properties but are overridden by per-query context values.
      </>
    ),
  },
  {
    name: 'queryBlocklist',
    type: 'json',
    info: (
      <>
        List of rules to block queries on brokers. Each rule can match by datasource, query type,
        and/or context parameters.
      </>
    ),
  },
  {
    name: 'perSegmentTimeoutConfig',
    type: 'json',
    info: (
      <>
        Per-datasource per-segment timeout configuration. Maps datasource name to timeout settings.
        When a query targets a configured datasource, the broker injects the per-segment timeout
        into the query context (unless the caller already set it explicitly). Set{' '}
        <code>monitorOnly: true</code> to log without enforcing.
      </>
    ),
  },
];
