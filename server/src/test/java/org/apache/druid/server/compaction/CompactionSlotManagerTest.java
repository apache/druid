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

import org.apache.druid.client.indexing.ClientCompactionRunnerInfo;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.client.indexing.ClientMSQContext;
import org.apache.druid.indexer.CompactionEngine;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class CompactionSlotManagerTest
{
  @Test
  public void test_computeSlotsRequiredForTask_forMsqTask_whenContextIsNull_returnsDefault()
  {
    Assertions.assertEquals(
        ClientMSQContext.DEFAULT_MAX_NUM_TASKS,
        CompactionSlotManager.computeSlotsRequiredForTask(
            createMsqTask(null)
        )
    );
  }

  @Test
  public void test_computeSlotsRequiredForTask_forMsqTask_whenContextHasMaxNumTasks_returnsValue()
  {
    Assertions.assertEquals(
        50,
        CompactionSlotManager.computeSlotsRequiredForTask(
            createMsqTask(Map.of(ClientMSQContext.CTX_MAX_NUM_TASKS, 50))
        )
    );
  }

  @Test
  public void test_computeSlotsRequiredForTask_forMsqTask_whenContextIsEmpty_returnsDefault()
  {
    Assertions.assertEquals(
        ClientMSQContext.DEFAULT_MAX_NUM_TASKS,
        CompactionSlotManager.computeSlotsRequiredForTask(
            createMsqTask(Map.of())
        )
    );
  }

  private ClientCompactionTaskQuery createMsqTask(Map<String, Object> context)
  {
    return new ClientCompactionTaskQuery(
        "msq_compact_1",
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        context,
        new ClientCompactionRunnerInfo(CompactionEngine.MSQ)
    );
  }
}
