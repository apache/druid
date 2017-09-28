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

import com.google.common.base.Predicate;
import io.druid.java.util.common.Pair;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;

class FilteringSegmentCallback implements ServerView.SegmentCallback
{

  private final ServerView.SegmentCallback callback;
  private final Predicate<Pair<DruidServerMetadata, DataSegment>> filter;

  FilteringSegmentCallback(ServerView.SegmentCallback callback, Predicate<Pair<DruidServerMetadata, DataSegment>> filter)
  {
    this.callback = callback;
    this.filter = filter;
  }

  @Override
  public ServerView.CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
  {
    final ServerView.CallbackAction action;
    if (filter.apply(Pair.of(server, segment))) {
      action = callback.segmentAdded(server, segment);
    } else {
      action = ServerView.CallbackAction.CONTINUE;
    }
    return action;
  }

  @Override
  public ServerView.CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
  {
    final ServerView.CallbackAction action;
    if (filter.apply(Pair.of(server, segment))) {
      action = callback.segmentRemoved(server, segment);
    } else {
      action = ServerView.CallbackAction.CONTINUE;
    }
    return action;
  }

  @Override
  public ServerView.CallbackAction segmentViewInitialized()
  {
    return callback.segmentViewInitialized();
  }
}
