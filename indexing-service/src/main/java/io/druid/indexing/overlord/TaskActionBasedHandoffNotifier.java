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

package io.druid.indexing.overlord;

import com.google.common.collect.Maps;
import com.metamx.common.Pair;
import com.metamx.common.logger.Logger;
import io.druid.concurrent.Execs;
import io.druid.indexing.common.actions.SegmentHandoffCheckAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.query.SegmentDescriptor;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifier;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TaskActionBasedHandoffNotifier implements SegmentHandoffNotifier
{
  private static final Logger log = new Logger(TaskActionBasedHandoffNotifier.class);

  private final Map<SegmentDescriptor, Pair<Executor, Runnable>> handOffCallbacks = Maps.newConcurrentMap();
  private final TaskActionClient taskActionClient;
  private volatile ScheduledExecutorService scheduledExecutor;
  private final long pollDurationMillis;

  public TaskActionBasedHandoffNotifier(
      TaskActionClient taskActionClient,
      TaskActionBasedHandoffNotifierConfig config
  )
  {
    this.taskActionClient = taskActionClient;
    this.pollDurationMillis = config.getPollDuration().getMillis();
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
    scheduledExecutor = Execs.scheduledSingleThreaded("plumber_scheduled_%d");
    scheduledExecutor.scheduleAtFixedRate(
        new Runnable()
        {
          @Override
          public void run()
          {
            Iterator<Map.Entry<SegmentDescriptor, Pair<Executor, Runnable>>> itr = handOffCallbacks.entrySet()
                                                                                                   .iterator();
            while (itr.hasNext()) {
              Map.Entry<SegmentDescriptor, Pair<Executor, Runnable>> entry = itr.next();
              try {
                if (taskActionClient.submit(new SegmentHandoffCheckAction(entry.getKey()))) {
                  entry.getValue().lhs.execute(entry.getValue().rhs);
                  itr.remove();
                }
              }
              catch (IOException e) {
                log.error("Error while checking Segment Handoff", e);
              }
            }
          }
        }, 0L, pollDurationMillis, TimeUnit.MILLISECONDS
    );
  }

  @Override
  public void stop()
  {
    scheduledExecutor.shutdown();
  }

  // Used in tests
  Map<SegmentDescriptor, Pair<Executor, Runnable>> getHandOffCallbacks()
  {
    return handOffCallbacks;
  }
}
