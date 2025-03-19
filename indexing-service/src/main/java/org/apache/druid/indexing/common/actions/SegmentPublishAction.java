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

package org.apache.druid.indexing.common.actions;

import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;

/**
 * Task action to publish segments. Contains common code used by insert, replace
 * and append actions.
 */
public abstract class SegmentPublishAction implements TaskAction<SegmentPublishResult>
{
  private static final int QUIET_RETRIES = 3;
  private static final int MAX_RETRIES = 5;

  @Override
  public SegmentPublishResult perform(Task task, TaskActionToolbox toolbox)
  {
    int attemptCount = 0;
    final String taskId = task.getId();

    // Retry until success or until max retries are exhausted
    SegmentPublishResult result = tryPublishSegments(task, toolbox);
    while (!result.isSuccess() && result.canRetry() && attemptCount++ < MAX_RETRIES) {
      awaitNextRetry(taskId, result, attemptCount);
      result = tryPublishSegments(task, toolbox);
    }

    IndexTaskUtils.emitSegmentPublishMetrics(result, task, toolbox);
    return result;
  }

  /**
   * Sleeps until the next attempt.
   */
  private static void awaitNextRetry(String taskId, SegmentPublishResult lastResult, int attemptCount)
  {
    try {
      RetryUtils.awaitNextRetry(
          new ISE(lastResult.getErrorMsg()),
          StringUtils.format(
              "Segment publish for task[%s] failed due to error[%s]",
              taskId, lastResult.getErrorMsg()
          ),
          attemptCount,
          MAX_RETRIES,
          attemptCount <= QUIET_RETRIES
      );
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Commits the segments provided in this task action if the underlying task
   * holds the required locks over these segments.
   */
  protected abstract SegmentPublishResult tryPublishSegments(
      Task task,
      TaskActionToolbox toolbox
  );
}
