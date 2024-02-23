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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.metadata.TaskLookup;
import org.junit.Assert;
import org.junit.Test;

public class TaskStorageUtilsTest
{
  @Test
  public void test_processTaskLookups_defaultLimit_defaultTime()
  {
    Assert.assertEquals(
        ImmutableMap.of(
            TaskLookup.TaskLookupType.ACTIVE,
            TaskLookup.ActiveTaskLookup.getInstance(),
            TaskLookup.TaskLookupType.COMPLETE,
            new TaskLookup.CompleteTaskLookup(null, DateTimes.of("2000"))
        ),
        TaskStorageUtils.processTaskLookups(
            ImmutableMap.of(
                TaskLookup.TaskLookupType.ACTIVE,
                TaskLookup.ActiveTaskLookup.getInstance(),
                TaskLookup.TaskLookupType.COMPLETE,
                TaskLookup.CompleteTaskLookup.of(null, null)
            ),
            DateTimes.of("2000")
        )
    );
  }

  @Test
  public void test_processTaskLookups_zeroCompleteTasks_defaultTime()
  {
    Assert.assertEquals(
        ImmutableMap.of(
            TaskLookup.TaskLookupType.ACTIVE,
            TaskLookup.ActiveTaskLookup.getInstance()
        ),
        TaskStorageUtils.processTaskLookups(
            ImmutableMap.of(
                TaskLookup.TaskLookupType.ACTIVE,
                TaskLookup.ActiveTaskLookup.getInstance(),
                TaskLookup.TaskLookupType.COMPLETE,
                new TaskLookup.CompleteTaskLookup(0, DateTimes.of("2000"))
            ),
            DateTimes.of("2000")
        )
    );
  }

  @Test
  public void test_processTaskLookups_oneCompleteTask_3000()
  {
    Assert.assertEquals(
        ImmutableMap.of(
            TaskLookup.TaskLookupType.ACTIVE,
            TaskLookup.ActiveTaskLookup.getInstance(),
            TaskLookup.TaskLookupType.COMPLETE,
            new TaskLookup.CompleteTaskLookup(1, DateTimes.of("3000"))
        ),
        TaskStorageUtils.processTaskLookups(
            ImmutableMap.of(
                TaskLookup.TaskLookupType.ACTIVE,
                TaskLookup.ActiveTaskLookup.getInstance(),
                TaskLookup.TaskLookupType.COMPLETE,
                new TaskLookup.CompleteTaskLookup(1, DateTimes.of("3000"))
            ),
            DateTimes.of("2000")
        )
    );
  }
}
