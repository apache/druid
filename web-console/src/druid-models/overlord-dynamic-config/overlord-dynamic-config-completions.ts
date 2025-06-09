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

// Affinity config completions for affinity field
export const AFFINITY_CONFIG_COMPLETIONS: JsonCompletionRule[] = [
  {
    path: '$',
    isObject: true,
    completions: [
      { value: 'affinity', documentation: 'Map of datasource names to lists of Middle Manager host:port values' },
      { value: 'strong', documentation: 'Whether tasks must be assigned to affinity-mapped workers' },
    ],
  },
  {
    path: '$.strong',
    completions: [
      { value: 'true', documentation: 'Tasks must be assigned to affinity-mapped workers' },
      { value: 'false', documentation: 'Affinity is preferred but not required (default)' },
    ],
  },
];

// Worker category spec completions for categoryMap field
export const CATEGORY_MAP_COMPLETIONS: JsonCompletionRule[] = [
  {
    path: '$',
    isObject: true,
    completions: [
      { value: 'index_kafka', documentation: 'Configuration for Kafka indexing tasks' },
      { value: 'index_kinesis', documentation: 'Configuration for Kinesis indexing tasks' },
      { value: 'index_parallel', documentation: 'Configuration for parallel indexing tasks' },
      { value: 'compact', documentation: 'Configuration for compaction tasks' },
      { value: 'query_controller', documentation: 'Configuration for MSQ query controller tasks' },
    ],
  },
  {
    path: /^\$\.[^.]+$/,
    isObject: true,
    completions: [
      { value: 'defaultCategory', documentation: 'Default worker category for this task type' },
      { value: 'categoryAffinity', documentation: 'Map of datasource names to specific worker categories' },
    ],
  },
];

// EC2 nodeData completions for nodeData field
export const NODE_DATA_COMPLETIONS: JsonCompletionRule[] = [
  {
    path: '$',
    isObject: true,
    completions: [
      { value: 'amiId', documentation: 'AWS AMI ID for new worker instances' },
      { value: 'instanceType', documentation: 'EC2 instance type (e.g., c5.2xlarge)' },
      { value: 'minInstances', documentation: 'Minimum instances to launch per auto-scaling event' },
      { value: 'maxInstances', documentation: 'Maximum instances to launch per auto-scaling event' },
      { value: 'securityGroupIds', documentation: 'List of security group IDs to assign to instances' },
      { value: 'keyName', documentation: 'EC2 key pair name for SSH access' },
      { value: 'subnetId', documentation: 'VPC subnet ID where instances will be launched' },
      { value: 'iamInstanceProfile', documentation: 'IAM instance profile for EC2 instances' },
      { value: 'userData', documentation: 'User data configuration for instance setup' },
    ],
  },
  {
    path: '$.instanceType',
    completions: [
      { value: 'c5.large', documentation: '2 vCPUs, 4 GiB RAM' },
      { value: 'c5.xlarge', documentation: '4 vCPUs, 8 GiB RAM' },
      { value: 'c5.2xlarge', documentation: '8 vCPUs, 16 GiB RAM' },
      { value: 'c5.4xlarge', documentation: '16 vCPUs, 32 GiB RAM' },
      { value: 'c5.9xlarge', documentation: '36 vCPUs, 72 GiB RAM' },
      { value: 'm5.large', documentation: '2 vCPUs, 8 GiB RAM' },
      { value: 'm5.xlarge', documentation: '4 vCPUs, 16 GiB RAM' },
      { value: 'm5.2xlarge', documentation: '8 vCPUs, 32 GiB RAM' },
      { value: 'm5.4xlarge', documentation: '16 vCPUs, 64 GiB RAM' },
    ],
  },
];

// EC2 userData completions for userData field
export const USER_DATA_COMPLETIONS: JsonCompletionRule[] = [
  {
    path: '$',
    isObject: true,
    completions: [
      { value: 'impl', documentation: 'Implementation type for user data' },
      { value: 'data', documentation: 'User data script or commands to run on instance startup' },
      { value: 'versionReplacementString', documentation: 'String to replace with actual Druid version' },
      { value: 'version', documentation: 'Druid version to use for replacement (null for current)' },
    ],
  },
  {
    path: '$.impl',
    completions: [
      { value: 'string', documentation: 'User data as plain string (default)' },
      { value: 'stringBase64', documentation: 'User data as base64 encoded string' },
    ],
  },
];