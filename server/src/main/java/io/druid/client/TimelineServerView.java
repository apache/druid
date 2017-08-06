/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.client;

import io.druid.client.selector.ServerSelector;
import io.druid.query.DataSource;
import io.druid.query.QueryRunner;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineLookup;

import javax.annotation.Nullable;
import java.util.concurrent.Executor;

/**
 */
public interface TimelineServerView extends ServerView
{
  @Nullable
  TimelineLookup<String, ServerSelector> getTimeline(DataSource dataSource);

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
    CallbackAction segmentAdded(final DruidServerMetadata server, final DataSegment segment);

    /**
     * Called when a segment has been removed from all servers and is no longer present in the timeline.
     *
     * @param segment the segment
     *
     * @return continue or unregister
     */
    CallbackAction segmentRemoved(final DataSegment segment);
  }
}
