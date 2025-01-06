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

package org.apache.druid.metadata.cache;

import org.apache.druid.timeline.SegmentTimeline;

import java.util.Map;

public interface SegmentsMetadataReadOnlyCache
{
  boolean isReady();

  Map<String, SegmentTimeline> getDataSourceToUsedSegmentTimeline();

  // TODO: Let's think about the API for a bit
  // What kind of methods are we going to need
  // We essentially want the same things that we fetch from the metadata store
  // i.e. Given some filters, we want to fetch a bunch of segments
  // And in most cases, we want to make a timeline out of those segments

  // Timeline helps filter on:
  // - visibility
  // - interval
  // But probably not with segments which are fully contained within an interval
  // We still need the other map because sometimes we want to fetch non-overshadowed stuff too
  // Do we, really? I think only Coordinator does that.
  // Coordinator has its own query to perform that operation.
  // In any case, we do have the map for other purposes, so we will use it whenever necessary

  // There has to be a common point
  // If cache is enabled - new flow
  // Otherwise go to old flow

  // The queries will have to move away from IndexerSQLMetadataStorageCoordinator because
  // - updates go both to cache and metadata store
  // -

  // - We will still need the new connector so that stuff is handled properly
  // - otherwise, there will be a lot of duplication of term logic and other cache logic
  // - IndexerSQL should ideally be agnostic of all of that

  // Alternative:
  // - All logic lives in IndexerSQL
  // - Just update segment allocation logic for now
  // - Other write operations simply update the cache if the cache is in READY state
  // - We would still need the leader info
}
