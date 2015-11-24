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

package io.druid.segment.realtime.plumber;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.Pair;
import com.metamx.common.logger.Logger;
import io.druid.client.FilteredServerView;
import io.druid.client.ServerView;
import io.druid.query.SegmentDescriptor;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executor;

public class ServerViewSegmentHandoffNotifier implements SegmentHandoffNotifier
{
  private static final Logger log = new Logger(ServerViewSegmentHandoffNotifier.class);

  private final FilteredServerView serverView;
  private final String dataSource;
  private final Map<SegmentDescriptor, Pair<Executor, Runnable>> handOffCallbacks = Maps.newConcurrentMap();


  public ServerViewSegmentHandoffNotifier(
      FilteredServerView serverView,
      String dataSource
  )
  {
    this.serverView = serverView;
    this.dataSource = dataSource;

  }


  @Override
  public void registerSegmentHandoffCallback(
      SegmentDescriptor descriptor, Executor exec, Runnable handOffRunnable
  )
  {
    handOffCallbacks.put(descriptor, new Pair<>(exec, handOffRunnable));
  }

  @Override
  public void start()
  {
    serverView.registerSegmentCallback(
        MoreExecutors.sameThreadExecutor(), new ServerView.BaseSegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentAdded(
              DruidServerMetadata server, DataSegment segment
          )
          {
            if (dataSource.equals(segment.getDataSource())) {
              Iterator<Map.Entry<SegmentDescriptor, Pair<Executor, Runnable>>> itr = handOffCallbacks.entrySet()
                                                                                                     .iterator();
              while (itr.hasNext()) {
                Map.Entry<SegmentDescriptor, Pair<Executor, Runnable>> entry = itr.next();
                if (segment.getInterval().contains(entry.getKey().getInterval())) {
                  log.info("Segment[%s] matches sink[%s] on server[%s]", segment, entry.getKey(), server);

                  final String segmentVersion = segment.getVersion();
                  if (segmentVersion.compareTo(entry.getKey().getVersion()) >= 0) {
                    log.info("Segment version[%s] >= sink version[%s]", segmentVersion, entry.getKey().getVersion());
                    entry.getValue().lhs.execute(entry.getValue().rhs);
                    itr.remove();
                  }
                }
              }
            }
            return ServerView.CallbackAction.CONTINUE;
          }
        },
        new Predicate<DataSegment>()
        {
          @Override
          public boolean apply(final DataSegment segment)
          {
            return
                dataSource.equals(segment.getDataSource())
                && Iterables.any(
                    handOffCallbacks.keySet(), new Predicate<SegmentDescriptor>()
                    {
                      @Override
                      public boolean apply(SegmentDescriptor sinkKey)
                      {
                        return segment.getShardSpec().getPartitionNum() == sinkKey.getPartitionNumber()
                               && segment.getInterval().contains(sinkKey.getInterval());
                      }
                    }
                );
          }
        }
    );
  }

  @Override
  public void stop()
  {
    // Nothing to cleanup
  }
}
