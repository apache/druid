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

package org.apache.druid.client;

import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineLookup;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

/**
 */
public interface TimelineServerView extends ServerView
{
  /**
   * Returns the timeline for a datasource, if it exists. The analysis object passed in must represent a scan-based
   * datasource of a single table.
   *
   * @param analysis data source analysis information
   *
   * @return timeline, if it exists
   *
   * @throws IllegalStateException if 'analysis' does not represent a scan-based datasource of a single table
   */
  Optional<? extends TimelineLookup<String, ServerSelector>> getTimeline(DataSourceAnalysis analysis);

  /**
   * Returns a list of {@link ImmutableDruidServer}
   */
  List<ImmutableDruidServer> getDruidServers();

  <T> QueryRunner<T> getQueryRunner(DruidServer server);

  /**
   * Register a callback for state changes in the timeline managed by this TimelineServerView. The callback will be
   * called after the relevant change is made to this TimelineServerView's timeline.
   *
   * @param exec     executor in which to run the callback
   * @param callback the callback
   */
  void registerTimelineCallback(Executor exec, TimelineCallback callback);

  interface TimelineCallback
  {
    /**
     * Called once, when the timeline has been initialized.
     *
     * @return continue or unregister
     */
    CallbackAction timelineInitialized();

    /**
     * Called when a segment on a particular server has been added to the timeline. May be called multiple times for
     * the same segment, if that segment is added on multiple servers.
     *
     * @param server  the server
     * @param segment the segment
     *
     * @return continue or unregister
     */
    CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment);

    /**
     * Called when a segment has been removed from all servers and is no longer present in the timeline.
     *
     * @param segment the segment
     *
     * @return continue or unregister
     */
    CallbackAction segmentRemoved(DataSegment segment);

    /**
     * Called when a segment is removed from a server. Note that the timeline can still have the segment, even though it's removed from given server.
     * {@link #segmentRemoved(DataSegment)} is the authority on when segment is removed from the timeline.
     *
     * @param server  The server that removed a segment
     * @param segment The segment that was removed
     *
     * @return continue or unregister
     */
    CallbackAction serverSegmentRemoved(DruidServerMetadata server, DataSegment segment);

    /**
     * Called when segment schema is announced.
     * Schema flow HttpServerInventoryView -> CoordinatorServerView -> CoordinatorSegmentMetadataCache
     * CoordinatorServerView simply delegates the schema information by invoking Timeline callback to metadata cache.
     *
     * @param segmentSchemas segment schema
     * @return continue or unregister
     */
    CallbackAction segmentSchemasAnnounced(SegmentSchemas segmentSchemas);
  }
}
