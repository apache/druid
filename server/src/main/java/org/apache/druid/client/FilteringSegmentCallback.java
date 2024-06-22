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

import com.google.common.base.Predicate;
import org.apache.druid.client.ServerView.CallbackAction;
import org.apache.druid.client.ServerView.SegmentCallback;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.DataSegment;

/**
 * A SegmentCallback that is called only when the given filter satisfies.
 * {@link  #segmentViewInitialized()} is an exception and always called
 * when the view is initialized without using the filter.
 * Callback methods return {@link CallbackAction#CONTINUE} when the filter does not satisfy.
 */
public class FilteringSegmentCallback implements SegmentCallback
{

  private final SegmentCallback callback;
  private final Predicate<Pair<DruidServerMetadata, DataSegment>> filter;

  public FilteringSegmentCallback(SegmentCallback callback, Predicate<Pair<DruidServerMetadata, DataSegment>> filter)
  {
    this.callback = callback;
    this.filter = filter;
  }

  @Override
  public CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
  {
    final CallbackAction action;
    if (filter.apply(Pair.of(server, segment))) {
      action = callback.segmentAdded(server, segment);
    } else {
      action = CallbackAction.CONTINUE;
    }
    return action;
  }

  @Override
  public CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
  {
    final CallbackAction action;
    if (filter.apply(Pair.of(server, segment))) {
      action = callback.segmentRemoved(server, segment);
    } else {
      action = CallbackAction.CONTINUE;
    }
    return action;
  }

  @Override
  public CallbackAction segmentViewInitialized()
  {
    return callback.segmentViewInitialized();
  }

  @Override
  public CallbackAction segmentSchemasAnnounced(SegmentSchemas segmentSchemas)
  {
    return CallbackAction.CONTINUE;
  }
}
