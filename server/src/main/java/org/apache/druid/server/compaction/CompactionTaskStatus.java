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

package org.apache.druid.server.compaction;

import org.apache.druid.indexer.TaskState;
import org.joda.time.DateTime;

public class CompactionTaskStatus
{
  private final TaskState state;
  private final DateTime updatedTime;
  private final int numConsecutiveFailures;

  public CompactionTaskStatus(
      TaskState state,
      DateTime updatedTime,
      int numConsecutiveFailures
  )
  {
    this.state = state;
    this.updatedTime = updatedTime;
    this.numConsecutiveFailures = numConsecutiveFailures;
  }

  public TaskState getState()
  {
    return state;
  }

  public DateTime getUpdatedTime()
  {
    return updatedTime;
  }

  public int getNumConsecutiveFailures()
  {
    return numConsecutiveFailures;
  }
}
