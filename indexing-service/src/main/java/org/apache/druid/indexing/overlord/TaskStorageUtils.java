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

package org.apache.druid.indexing.overlord;

import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.metadata.TaskLookup;
import org.joda.time.DateTime;

import java.util.LinkedHashMap;
import java.util.Map;

public class TaskStorageUtils
{
  private TaskStorageUtils()
  {
    // No instantiation.
  }

  /**
   * Process a map of {@link TaskLookup} to apply {@link TaskStorageConfig#getRecentlyFinishedThreshold()}, and to
   * remove lookups for which {@link TaskLookup#isNil()}.
   *
   * @param taskLookups          lookups from {@link TaskStorage#getTaskInfos(Map, String)}
   * @param minCreationTimestamp minimum creation time based on {@link TaskStorageConfig#getRecentlyFinishedThreshold()}
   */
  public static Map<TaskLookup.TaskLookupType, TaskLookup> processTaskLookups(
      final Map<TaskLookup.TaskLookupType, TaskLookup> taskLookups,
      final DateTime minCreationTimestamp
  )
  {
    final Map<TaskLookup.TaskLookupType, TaskLookup> retVal = new LinkedHashMap<>();

    for (Map.Entry<TaskLookup.TaskLookupType, TaskLookup> entry : taskLookups.entrySet()) {
      if (!entry.getValue().isNil()) {
        if (entry.getKey() == TaskLookup.TaskLookupType.COMPLETE) {
          TaskLookup.CompleteTaskLookup completeTaskLookup = (TaskLookup.CompleteTaskLookup) entry.getValue();
          retVal.put(
              entry.getKey(),
              completeTaskLookup.withMinTimestampIfAbsent(minCreationTimestamp)
          );
        } else {
          retVal.put(entry.getKey(), entry.getValue());
        }
      }
    }

    return retVal;
  }
}
