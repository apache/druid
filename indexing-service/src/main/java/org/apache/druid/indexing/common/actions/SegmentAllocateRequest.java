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

import org.apache.druid.indexing.common.task.Task;

/**
 * Request received by the overlord for segment allocation.
 */
public class SegmentAllocateRequest
{
  private final Task task;
  private final SegmentAllocateAction action;
  private final int maxAttempts;

  private int attempts;

  public SegmentAllocateRequest(Task task, SegmentAllocateAction action, int maxAttempts)
  {
    this.task = task;
    this.action = action;
    this.maxAttempts = maxAttempts;
  }

  public Task getTask()
  {
    return task;
  }

  public SegmentAllocateAction getAction()
  {
    return action;
  }

  public void incrementAttempts()
  {
    ++attempts;
  }

  public boolean canRetry()
  {
    return attempts < maxAttempts;
  }

  public int getAttempts()
  {
    return attempts;
  }

}
