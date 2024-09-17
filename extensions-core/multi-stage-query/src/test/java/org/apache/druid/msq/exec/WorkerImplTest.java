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

package org.apache.druid.msq.exec;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.QueryContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class WorkerImplTest
{
  @Test
  public void test_makeWorkOrderToUse_nothingMissing()
  {
    final WorkOrder workOrder = new WorkOrder(
        QueryValidatorTest.createQueryDefinition(10, 2),
        0,
        0,
        Collections.singletonList(() -> 1),
        null,
        null,
        OutputChannelMode.MEMORY,
        ImmutableMap.of("foo", "bar")
    );

    Assert.assertSame(
        workOrder,
        WorkerImpl.makeWorkOrderToUse(
            workOrder,
            QueryContext.of(ImmutableMap.of("foo", "baz")) /* Conflicts with workOrder context; should be ignored */
        )
    );
  }

  @Test
  public void test_makeWorkOrderToUse_missingOutputChannelModeAndWorkerContext()
  {
    final Map<String, Object> taskContext =
        ImmutableMap.of("foo", "bar", MultiStageQueryContext.CTX_DURABLE_SHUFFLE_STORAGE, true);

    final WorkOrder workOrder = new WorkOrder(
        QueryValidatorTest.createQueryDefinition(10, 2),
        1,
        2,
        Collections.singletonList(() -> 1),
        null,
        null,
        null,
        null
    );

    Assert.assertEquals(
        new WorkOrder(
            workOrder.getQueryDefinition(),
            workOrder.getStageNumber(),
            workOrder.getWorkerNumber(),
            workOrder.getInputs(),
            null,
            null,
            OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE,
            taskContext
        ),
        WorkerImpl.makeWorkOrderToUse(workOrder, QueryContext.of(taskContext))
    );
  }
}
