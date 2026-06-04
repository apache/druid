/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.coordinator.rules;

import org.apache.druid.timeline.DataSegment;

import java.util.Map;

/**
 * Signal returned by {@link Rule#run} that the entire shard group containing {@link #matchedSegment} should be
 * inspected for uniform partial-load placement. The {@code RunRules} duty collects these and, after all rules have
 * run, dispatches {@link PartialLoadMatcher#emptyMatch} loads to siblings that did not get a positive match from
 * the same {@link #matcher}.
 *
 * <p>Used to handle asymmetric matchers (e.g. {@link ClusterGroupPartialLoadMatcher} over range-partitioned segments)
 * where different partitions of a shard group resolve to different load specs and the broker would otherwise drop
 * the group as incomplete via {@code PartitionHolder.isComplete()}.
 */
public record ShardGroupFollowup(
    DataSegment matchedSegment,
    PartialLoadMatcher matcher,
    Map<String, Integer> tieredReplicants
) implements RuleRunResult
{
}
