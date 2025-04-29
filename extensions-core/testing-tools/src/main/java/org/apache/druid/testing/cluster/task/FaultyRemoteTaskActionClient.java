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

package org.apache.druid.testing.cluster.task;

import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalAppendAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalReplaceAction;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.cluster.ClusterTestingTaskConfig;
import org.joda.time.Duration;

import java.io.IOException;

public class FaultyRemoteTaskActionClient implements TaskActionClient
{
  private static final Logger log = new Logger(FaultyRemoteTaskActionClient.class);

  private final TaskActionClient delegate;
  private final ClusterTestingTaskConfig.TaskActionClientConfig config;

  FaultyRemoteTaskActionClient(
      ClusterTestingTaskConfig.TaskActionClientConfig config,
      TaskActionClient delegate
  )
  {
    this.config = config;
    this.delegate = delegate;
  }

  @Override
  public <RetType> RetType submit(TaskAction<RetType> taskAction) throws IOException
  {
    if (taskAction instanceof SegmentAllocateAction
        && config.getSegmentAllocateDelay() != null) {
      log.warn("Sleeping for duration[%s] before allocating segments.", config.getSegmentAllocateDelay());
      sleep(config.getSegmentAllocateDelay());
    }

    if (isPublishAction(taskAction) && config.getSegmentPublishDelay() != null) {
      log.warn("Sleeping for duration[%s] before publishing segments.", config.getSegmentPublishDelay());
      sleep(config.getSegmentPublishDelay());
    }

    return delegate.submit(taskAction);
  }

  private static <R> boolean isPublishAction(TaskAction<R> taskAction)
  {
    return taskAction instanceof SegmentTransactionalInsertAction
           || taskAction instanceof SegmentTransactionalAppendAction
           || taskAction instanceof SegmentTransactionalReplaceAction;
  }

  private static void sleep(Duration duration)
  {
    try {
      Thread.sleep(duration.getMillis());
    }
    catch (InterruptedException e) {
      log.info("Interrupted while sleeping before task action.");
    }
  }
}
