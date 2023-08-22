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
import React from 'react';

import type { Field } from '../../components';
import { ExternalLink } from '../../components';
import { getLink } from '../../links';

export interface CoordinatorDynamicConfig {
  maxSegmentsToMove?: number;
  balancerComputeThreads?: number;
  killAllDataSources?: boolean;
  killDataSourceWhitelist?: string[];
  killTaskSlotRatio?: number;
  maxKillTaskSlots?: number;
  killPendingSegmentsSkipList?: string[];
  maxSegmentsInNodeLoadingQueue?: number;
  mergeBytesLimit?: number;
  mergeSegmentsLimit?: number;
  millisToWaitBeforeDeleting?: number;
  replicantLifetime?: number;
  replicationThrottleLimit?: number;
  decommissioningNodes?: string[];
  decommissioningMaxPercentOfMaxSegmentsToMove?: number;
  pauseCoordination?: boolean;
  maxNonPrimaryReplicantsToLoad?: number;
  replicateAfterLoadTimeout?: boolean;
  useRoundRobinSegmentAssignment?: boolean;
  smartSegmentLoading?: boolean;

  // Undocumented
  debugDimensions?: any;
}

export const COORDINATOR_DYNAMIC_CONFIG_FIELDS: Field<CoordinatorDynamicConfig>[] = [
  {
    name: 'pauseCoordination',
    type: 'boolean',
    defaultValue: false,
    info: (
      <>
        Boolean flag for whether or not the coordinator should execute its various duties of
        coordinating the cluster. Setting this to true essentially pauses all coordination work
        while allowing the API to remain up. Duties that are paused include all classes that
        implement the <Code>CoordinatorDuty</Code> interface. Such duties include: Segment
        balancing, Segment compaction, Submitting kill tasks for unused segments (if enabled),
        Logging of used segments in the cluster, Marking of newly unused or overshadowed segments,
        Matching and execution of load/drop rules for used segments, Unloading segments that are no
        longer marked as used from Historical servers. An example of when an admin may want to pause
        coordination would be if they are doing deep storage maintenance on HDFS Name Nodes with
        downtime and don&apos;t want the coordinator to be directing Historical Nodes to hit the
        Name Node with API requests until maintenance is done and the deep store is declared healthy
        for use again.
      </>
    ),
  },

  // Start "smart" segment loading section

  {
    name: 'smartSegmentLoading',
    type: 'boolean',
    defaultValue: true,
    info: (
      <>
        Enables{' '}
        <ExternalLink href={`${getLink('DOCS')}/configuration#smart-segment-loading`}>
          &quot;smart&quot; segment loading mode
        </ExternalLink>{' '}
        which dynamically computes the optimal values of several properties that maximize
        Coordinator performance.
      </>
    ),
  },
  {
    name: 'maxSegmentsToMove',
    type: 'number',
    defaultValue: 100,
    defined: cdc => (cdc.smartSegmentLoading === false ? true : undefined),
    info: <>The maximum number of segments that can be moved at any given time.</>,
  },
  {
    name: 'maxSegmentsInNodeLoadingQueue',
    type: 'number',
    defaultValue: 500,
    defined: cdc => (cdc.smartSegmentLoading === false ? true : undefined),
    info: (
      <>
        The maximum number of segments that could be queued for loading to any given server. This
        parameter could be used to speed up segments loading process, especially if there are
        &quot;slow&quot; nodes in the cluster (with low loading speed) or if too much segments
        scheduled to be replicated to some particular node (faster loading could be preferred to
        better segments distribution). Desired value depends on segments loading speed, acceptable
        replication time and number of nodes. Value 1000 could be a start point for a rather big
        cluster. Default value is 500.
      </>
    ),
  },
  {
    name: 'useRoundRobinSegmentAssignment',
    type: 'boolean',
    defaultValue: true,
    defined: cdc => (cdc.smartSegmentLoading === false ? true : undefined),
    info: (
      <>
        Boolean flag for whether segments should be assigned to historicals in a round robin
        fashion. When disabled, segment assignment is done using the chosen balancer strategy. When
        enabled, this can speed up segment assignments leaving balancing to move the segments to
        their optimal locations (based on the balancer strategy) lazily.
      </>
    ),
  },
  {
    name: 'replicationThrottleLimit',
    type: 'number',
    defaultValue: 500,
    defined: cdc => (cdc.smartSegmentLoading === false ? true : undefined),
    info: <>The maximum number of segments that can be replicated at one time.</>,
  },
  {
    name: 'replicantLifetime',
    type: 'number',
    defaultValue: 15,
    defined: cdc => (cdc.smartSegmentLoading === false ? true : undefined),
    info: (
      <>
        The maximum number of Coordinator runs for which a segment can wait in the load queue of a
        Historical before Druid raises an alert.
      </>
    ),
  },
  {
    name: 'maxNonPrimaryReplicantsToLoad',
    type: 'number',
    defaultValue: 2147483647,
    defined: cdc => (cdc.smartSegmentLoading === false ? true : undefined),
    info: (
      <>
        The maximum number of non-primary replicants to load in a single Coordinator cycle. Once
        this limit is hit, only primary replicants will be loaded for the remainder of the cycle.
        Tuning this value lower can help reduce the delay in loading primary segments when the
        cluster has a very large number of non-primary replicants to load (such as when a single
        historical drops out of the cluster leaving many under-replicated segments).
      </>
    ),
  },
  {
    name: 'decommissioningMaxPercentOfMaxSegmentsToMove',
    type: 'number',
    defaultValue: 70,
    defined: cdc => (cdc.smartSegmentLoading === false ? true : undefined),
    info: (
      <>
        <p>
          Upper limit of segments the Coordinator can move from decommissioning servers to active
          non-decommissioning servers during a single run. This value is relative to the total
          maximum number of segments that can be moved at any given time based upon the value of
          <Code>maxSegmentsToMove</Code>.
        </p>
        <p>
          If <Code>decommissioningMaxPercentOfMaxSegmentsToMove</Code> is 0, the Coordinator does
          not move segments to decommissioning servers, effectively putting them in a type of
          &quot;maintenance&quot; mode. In this case, decommissioning servers do not participate in
          balancing or assignment by load rules. The Coordinator still considers segments on
          decommissioning servers as candidates to replicate on active servers.
        </p>
        <p>
          Decommissioning can stall if there are no available active servers to move the segments
          to. You can use the maximum percent of decommissioning segment movements to prioritize
          balancing or to decrease commissioning time to prevent active servers from being
          overloaded. The value must be between 0 and 100.
        </p>
      </>
    ),
  },

  // End "smart" segment loading section

  {
    name: 'decommissioningNodes',
    type: 'string-array',
    emptyValue: [],
    info: (
      <>
        List of historical services to &apos;decommission&apos;. Coordinator will not assign new
        segments to &apos;decommissioning&apos; services, and segments will be moved away from them
        to be placed on non-decommissioning services at the maximum rate specified by{' '}
        <Code>decommissioningMaxPercentOfMaxSegmentsToMove</Code>.
      </>
    ),
  },
  {
    name: 'killDataSourceWhitelist',
    label: 'Kill datasource whitelist',
    type: 'string-array',
    emptyValue: [],
    info: (
      <>
        List of dataSources for which kill tasks are sent if property{' '}
        <Code>druid.coordinator.kill.on</Code> is true. This can be a list of comma-separated
        dataSources or a JSON array.
      </>
    ),
  },
  {
    name: 'killPendingSegmentsSkipList',
    type: 'string-array',
    emptyValue: [],
    info: (
      <>
        List of dataSources for which pendingSegments are NOT cleaned up if property{' '}
        <Code>druid.coordinator.kill.pendingSegments.on</Code> is true. This can be a list of
        comma-separated dataSources or a JSON array.
      </>
    ),
  },
  {
    name: 'killTaskSlotRatio',
    type: 'ratio',
    defaultValue: 1,
    info: (
      <>
        Ratio of total available task slots, including autoscaling if applicable that will be
        allowed for kill tasks. This limit only applies for kill tasks that are spawned
        automatically by the Coordinator&apos;s auto kill duty, which is enabled when
        <Code>druid.coordinator.kill.on</Code> is true.
      </>
    ),
  },
  {
    name: 'maxKillTaskSlots',
    type: 'number',
    defaultValue: 2147483647,
    info: (
      <>
        Maximum number of tasks that will be allowed for kill tasks. This limit only applies for
        kill tasks that are spawned automatically by the Coordinator&apos;s auto kill duty, which is
        enabled when <Code>druid.coordinator.kill.on</Code> is true.
      </>
    ),
    min: 1,
  },
  {
    name: 'balancerComputeThreads',
    type: 'number',
    defaultValue: 1,
    info: (
      <>
        Thread pool size for computing moving cost of segments during segment balancing. Consider
        increasing this if you have a lot of segments and moving segments begins to stall.
      </>
    ),
  },
  {
    name: 'mergeBytesLimit',
    type: 'size-bytes',
    defaultValue: 524288000,
    info: <>The maximum total uncompressed size in bytes of segments to merge.</>,
  },
  {
    name: 'mergeSegmentsLimit',
    type: 'number',
    defaultValue: 100,
    info: <>The maximum number of segments that can be in a single append task.</>,
  },
  {
    name: 'millisToWaitBeforeDeleting',
    type: 'number',
    defaultValue: 900000,
    info: (
      <>
        How long does the Coordinator need to be a leader before it can start marking overshadowed
        segments as unused in metadata storage.
      </>
    ),
  },
  {
    name: 'replicateAfterLoadTimeout',
    type: 'boolean',
    defaultValue: false,
    info: (
      <>
        Boolean flag for whether or not additional replication is needed for segments that have
        failed to load due to the expiry of <Code>druid.coordinator.load.timeout</Code>. If this is
        set to true, the coordinator will attempt to replicate the failed segment on a different
        historical server. This helps improve the segment availability if there are a few slow
        historicals in the cluster. However, the slow historical may still load the segment later
        and the coordinator may issue drop requests if the segment is over-replicated.
      </>
    ),
  },
];
