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

import type { JsonCompletionRule } from '../../utils';

export const OVERLORD_DYNAMIC_CONFIG_COMPLETIONS: JsonCompletionRule[] = [
  // Root level properties
  {
    path: '$',
    isObject: true,
    completions: [
      {
        value: 'selectStrategy',
        documentation: 'Configuration for how to assign tasks to Middle Managers',
      },
      {
        value: 'autoScaler',
        documentation: 'Auto-scaling configuration (only used if autoscaling is enabled)',
      },
    ],
  },
  // selectStrategy object properties
  {
    path: '$.selectStrategy',
    isObject: true,
    completions: [{ value: 'type', documentation: 'Worker selection strategy type' }],
  },
  // selectStrategy.type values
  {
    path: '$.selectStrategy.type',
    completions: [
      {
        value: 'equalDistribution',
        documentation: 'Evenly distribute tasks across Middle Managers (default)',
      },
      {
        value: 'equalDistributionWithCategorySpec',
        documentation: 'Equal distribution with worker category specification',
      },
      {
        value: 'fillCapacity',
        documentation: 'Fill workers to capacity (useful for auto-scaling)',
      },
      {
        value: 'fillCapacityWithCategorySpec',
        documentation: 'Fill capacity with worker category specification',
      },
      {
        value: 'javascript',
        documentation: 'Custom JavaScript-based worker selection (prototyping only)',
      },
    ],
  },
  // affinityConfig for equalDistribution and fillCapacity
  {
    path: '$.selectStrategy',
    isObject: true,
    condition: obj => obj.type === 'equalDistribution' || obj.type === 'fillCapacity',
    completions: [
      { value: 'affinityConfig', documentation: 'Affinity configuration for worker assignment' },
    ],
  },
  // affinityConfig object properties
  {
    path: '$.selectStrategy.affinityConfig',
    isObject: true,
    completions: [
      { value: 'affinity', documentation: 'Map of datasource names to preferred worker hosts' },
      {
        value: 'strong',
        documentation: 'Whether affinity is strong (tasks wait for preferred workers)',
      },
    ],
  },
  // affinityConfig.strong values
  {
    path: '$.selectStrategy.affinityConfig.strong',
    completions: [
      { value: 'true', documentation: 'Strong affinity - tasks wait for preferred workers' },
      {
        value: 'false',
        documentation:
          'Weak affinity - use any available worker if preferred unavailable (default)',
      },
    ],
  },
  // workerCategorySpec for category-based strategies
  {
    path: '$.selectStrategy',
    isObject: true,
    condition: obj =>
      obj.type === 'equalDistributionWithCategorySpec' ||
      obj.type === 'fillCapacityWithCategorySpec',
    completions: [
      {
        value: 'workerCategorySpec',
        documentation: 'Worker category specification for task assignment',
      },
    ],
  },
  // workerCategorySpec object properties
  {
    path: '$.selectStrategy.workerCategorySpec',
    isObject: true,
    completions: [
      { value: 'categoryMap', documentation: 'Map of task types to category configurations' },
      { value: 'strong', documentation: 'Whether category affinity is strong' },
    ],
  },
  // workerCategorySpec.strong values
  {
    path: '$.selectStrategy.workerCategorySpec.strong',
    completions: [
      {
        value: 'true',
        documentation: 'Strong category affinity - tasks wait for preferred category workers',
      },
      {
        value: 'false',
        documentation:
          'Weak category affinity - use any available worker if preferred unavailable (default)',
      },
    ],
  },
  // function for javascript strategy
  {
    path: '$.selectStrategy',
    isObject: true,
    condition: obj => obj.type === 'javascript',
    completions: [
      { value: 'function', documentation: 'JavaScript function for custom worker selection logic' },
    ],
  },
  // autoScaler object properties
  {
    path: '$.autoScaler',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Auto-scaler type (ec2 or gce)' },
      { value: 'minNumWorkers', documentation: 'Minimum number of workers' },
      { value: 'maxNumWorkers', documentation: 'Maximum number of workers' },
      { value: 'envConfig', documentation: 'Environment-specific configuration' },
    ],
  },
  // autoScaler.type values
  {
    path: '$.autoScaler.type',
    completions: [
      { value: 'ec2', documentation: 'Amazon EC2 auto-scaling' },
      { value: 'gce', documentation: 'Google Compute Engine auto-scaling' },
    ],
  },
  // minNumWorkers values
  {
    path: '$.autoScaler.minNumWorkers',
    completions: [
      { value: '0', documentation: 'No minimum workers' },
      { value: '1', documentation: '1 minimum worker' },
      { value: '2', documentation: '2 minimum workers' },
      { value: '5', documentation: '5 minimum workers' },
    ],
  },
  // maxNumWorkers values
  {
    path: '$.autoScaler.maxNumWorkers',
    completions: [
      { value: '5', documentation: '5 maximum workers' },
      { value: '10', documentation: '10 maximum workers' },
      { value: '20', documentation: '20 maximum workers' },
      { value: '50', documentation: '50 maximum workers' },
    ],
  },
  // envConfig for EC2
  {
    path: '$.autoScaler.envConfig',
    isObject: true,
    condition: obj => obj.type === 'ec2',
    completions: [
      { value: 'availabilityZone', documentation: 'AWS availability zone' },
      { value: 'nodeData', documentation: 'Node configuration data for EC2 instances' },
      { value: 'userData', documentation: 'User data for EC2 instance initialization' },
    ],
  },
  // envConfig for GCE
  {
    path: '$.autoScaler.envConfig',
    isObject: true,
    condition: obj => obj.type === 'gce',
    completions: [
      { value: 'numInstances', documentation: 'Number of instances to create' },
      { value: 'projectId', documentation: 'Google Cloud project ID' },
      { value: 'zoneName', documentation: 'Google Cloud zone name' },
      { value: 'managedInstanceGroupName', documentation: 'Name of the managed instance group' },
    ],
  },
  // Common EC2 availability zones
  {
    path: '$.autoScaler.envConfig.availabilityZone',
    completions: [
      { value: 'us-east-1a', documentation: 'US East (N. Virginia) Zone A' },
      { value: 'us-east-1b', documentation: 'US East (N. Virginia) Zone B' },
      { value: 'us-west-2a', documentation: 'US West (Oregon) Zone A' },
      { value: 'us-west-2b', documentation: 'US West (Oregon) Zone B' },
      { value: 'eu-west-1a', documentation: 'Europe (Ireland) Zone A' },
    ],
  },
  // Common GCE zones
  {
    path: '$.autoScaler.envConfig.zoneName',
    completions: [
      { value: 'us-central1-a', documentation: 'US Central Zone A' },
      { value: 'us-central1-b', documentation: 'US Central Zone B' },
      { value: 'us-east1-a', documentation: 'US East Zone A' },
      { value: 'europe-west1-a', documentation: 'Europe West Zone A' },
    ],
  },
  // Common task types for categoryMap
  {
    path: '$.selectStrategy.workerCategorySpec.categoryMap',
    isObject: true,
    completions: [
      { value: 'index_kafka', documentation: 'Kafka indexing tasks' },
      { value: 'index_kinesis', documentation: 'Kinesis indexing tasks' },
      { value: 'index_parallel', documentation: 'Parallel batch indexing tasks' },
      { value: 'index_hadoop', documentation: 'Hadoop batch indexing tasks' },
    ],
  },
  // Category configuration properties
  {
    path: /^\$\.selectStrategy\.workerCategorySpec\.categoryMap\.[^.]+$/,
    isObject: true,
    completions: [
      { value: 'defaultCategory', documentation: 'Default worker category for this task type' },
      { value: 'categoryAffinity', documentation: 'Map of datasources to specific categories' },
    ],
  },
];
