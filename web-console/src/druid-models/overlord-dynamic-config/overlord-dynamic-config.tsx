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

import { Callout } from '@blueprintjs/core';
import React from 'react';

import type { Field } from '../../components';
import { deepGet, oneOf } from '../../utils';

export interface OverlordDynamicConfig {
  selectStrategy?: {
    type: string;
    affinityConfig?: AffinityConfig;
    workerCategorySpec?: WorkerCategorySpec;
  };
  autoScaler?: AutoScalerConfig;
}

export interface AffinityConfig {
  affinity?: Record<string, string[]>;
  strong?: boolean;
}

export interface WorkerCategorySpec {
  categoryMap?: Record<string, any>;
  strong?: boolean;
}

export interface AutoScalerConfig {
  type: string;
  minNumWorkers: number;
  maxNumWorkers: number;
  envConfig: {
    // ec2
    availabilityZone?: string;
    nodeData?: Record<string, any>;
    userData?: Record<string, any>;

    // gce
    numInstances?: number;
    projectId?: string;
    zoneName?: string;
    managedInstanceGroupName?: string;
  };
}

export const OVERLORD_DYNAMIC_CONFIG_FIELDS: Field<OverlordDynamicConfig>[] = [
  {
    name: 'selectStrategy.type',
    type: 'string',
    defaultValue: 'equalDistribution',
    suggestions: [
      'equalDistribution',
      'equalDistributionWithCategorySpec',
      'fillCapacity',
      'fillCapacityWithCategorySpec',
    ],
  },

  // AffinityConfig
  {
    name: 'selectStrategy.affinityConfig.affinity',
    type: 'json',
    placeholder: `{"datasource1":["host1:port","host2:port"], "datasource2":["host3:port"]}`,
    defined: c =>
      oneOf(
        deepGet(c, 'selectStrategy.type') ?? 'equalDistribution',
        'equalDistribution',
        'fillCapacity',
      ),
    info: (
      <>
        <p>An example affinity config might look like:</p>
        <Callout className="code-block">
          {`{
  "datasource1": ["host1:port", "host2:port"],
  "datasource2": ["host3:port"]
}`}
        </Callout>
      </>
    ),
  },
  {
    name: 'selectStrategy.affinityConfig.strong',
    type: 'boolean',
    defaultValue: false,
    defined: c =>
      oneOf(
        deepGet(c, 'selectStrategy.type') ?? 'equalDistribution',
        'equalDistribution',
        'fillCapacity',
      ),
  },

  // WorkerCategorySpec
  {
    name: 'selectStrategy.workerCategorySpec.categoryMap',
    type: 'json',
    defaultValue: '{}',
    defined: c =>
      oneOf(
        deepGet(c, 'selectStrategy.type'),
        'equalDistributionWithCategorySpec',
        'fillCapacityWithCategorySpec',
      ),
    info: (
      <>
        <p>An example category map might look like:</p>
        <Callout className="code-block">
          {`{
  "index_kafka": {
    "defaultCategory": "category1",
    "categoryAffinity": {
      "datasource1": "category2"
    }
  }
}`}
        </Callout>
      </>
    ),
  },
  {
    name: 'selectStrategy.workerCategorySpec.strong',
    type: 'boolean',
    defaultValue: false,
    defined: c =>
      oneOf(
        deepGet(c, 'selectStrategy.type'),
        'equalDistributionWithCategorySpec',
        'fillCapacityWithCategorySpec',
      ),
  },

  // javascript
  {
    name: 'selectStrategy.workerCategorySpec.function',
    type: 'string',
    multiline: true,
    placeholder: `function(config, zkWorkers, task) { ... }`,
    defined: c => deepGet(c, 'selectStrategy.type') === 'javascript',
  },

  {
    name: 'autoScaler.type',
    label: 'Auto scaler type',
    type: 'string',
    suggestions: [undefined, 'ec2', 'gce'],
    defined: c => oneOf(deepGet(c, 'selectStrategy.type'), 'fillCapacity', 'javascript'),
  },
  {
    name: 'autoScaler.minNumWorkers',
    label: 'Auto scaler min workers',
    type: 'number',
    defaultValue: 0,
    defined: c => oneOf(deepGet(c, 'autoScaler.type'), 'ec2', 'gce'),
  },
  {
    name: 'autoScaler.maxNumWorkers',
    label: 'Auto scaler max workers',
    type: 'number',
    defaultValue: 0,
    defined: c => oneOf(deepGet(c, 'autoScaler.type'), 'ec2', 'gce'),
  },

  // EC2
  {
    name: 'autoScaler.envConfig.availabilityZone',
    label: 'Auto scaler availability zone',
    type: 'string',
    defined: c => deepGet(c, 'autoScaler.type') === 'ec2',
  },
  {
    name: 'autoScaler.envConfig.nodeData',
    label: 'Auto scaler node data',
    type: 'json',
    defined: c => deepGet(c, 'autoScaler.type') === 'ec2',
    required: true,
    info: 'A JSON object that describes how to launch new nodes.',
  },
  {
    name: 'autoScaler.envConfig.userData',
    label: 'Auto scaler user data',
    type: 'json',
    defined: c => deepGet(c, 'autoScaler.type') === 'ec2',
  },

  // GCE
  {
    name: 'autoScaler.envConfig.numInstances',
    type: 'number',
    defined: c => deepGet(c, 'autoScaler.type') === 'gce',
  },
  {
    name: 'autoScaler.envConfig.projectId',
    type: 'string',
    defined: c => deepGet(c, 'autoScaler.type') === 'gce',
  },
  {
    name: 'autoScaler.envConfig.zoneName',
    type: 'string',
    defined: c => deepGet(c, 'autoScaler.type') === 'gce',
  },
  {
    name: 'autoScaler.envConfig.managedInstanceGroupName',
    type: 'string',
    defined: c => deepGet(c, 'autoScaler.type') === 'gce',
  },
];
