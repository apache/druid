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

import { Code } from '@blueprintjs/core';

import type { Field } from '../../components';

export interface BrokerDynamicConfig {
  queryBlocklist?: QueryBlocklistRule[];
  blacklistedDataNodes?: string[];
}

export interface QueryBlocklistRule {
  ruleName: string;
  dataSources?: string[];
  queryTypes?: string[];
  contextMatches?: Record<string, string>;
}

export const BROKER_DYNAMIC_CONFIG_FIELDS: Field<BrokerDynamicConfig>[] = [
  {
    name: 'blacklistedDataNodes',
    type: 'string-array',
    emptyValue: [],
    info: (
      <>
        List of data node host:port strings (e.g. <Code>historical1:8083</Code>) to exclude from
        query planning. Applies to all data node types (historicals, peons, indexers).
      </>
    ),
  },
  {
    name: 'queryBlocklist',
    type: 'json',
    height: '30vh',
    info: (
      <>
        List of rules to block queries on brokers. Each rule must have a <Code>ruleName</Code> and
        at least one of: <Code>dataSources</Code>, <Code>queryTypes</Code>, or{' '}
        <Code>contextMatches</Code>.
      </>
    ),
  },
];
