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

package org.apache.druid.msq.kernel.controller;

import com.google.common.collect.ImmutableList;
import org.apache.druid.msq.exec.OutputChannelMode;
import org.apache.druid.msq.indexing.destination.DurableStorageMSQDestination;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.ShuffleKind;
import org.apache.druid.msq.kernel.StageId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

public class ControllerQueryKernelUtilsTest
{
  @Test
  public void test_computeStageGroups_multiPronged()
  {
    final QueryDefinition queryDef = makeMultiProngedQueryDefinition();

    Assert.assertEquals(
        ImmutableList.of(
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 0),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 1),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 2),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 3),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 4),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 5),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 6)
        ),
        ControllerQueryKernelUtils.computeStageGroups(
            queryDef,
            ControllerQueryKernelConfig
                .builder()
                .maxRetainedPartitionSketchBytes(1)
                .maxConcurrentStages(1)
                .pipeline(false)
                .faultTolerance(false)
                .destination(TaskReportMSQDestination.instance())
                .build()
        )
    );
  }

  @Test
  public void test_computeStageGroups_multiPronged_pipeline_twoAtOnce()
  {
    final QueryDefinition queryDef = makeMultiProngedQueryDefinition();

    Assert.assertEquals(
        ImmutableList.of(
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 0),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 1),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 2, 4),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 3, 5),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.MEMORY, 6)
        ),
        ControllerQueryKernelUtils.computeStageGroups(
            queryDef,
            ControllerQueryKernelConfig
                .builder()
                .maxRetainedPartitionSketchBytes(1)
                .maxConcurrentStages(2)
                .pipeline(true)
                .faultTolerance(false)
                .destination(TaskReportMSQDestination.instance())
                .build()
        )
    );
  }

  @Test
  public void test_computeStageGroups_linearWithoutShuffle()
  {
    final QueryDefinition queryDef = makeLinearQueryDefinitionWithoutShuffle();

    Assert.assertEquals(
        ImmutableList.of(
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 0),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 1),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 2),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 3)
        ),
        ControllerQueryKernelUtils.computeStageGroups(
            queryDef,
            ControllerQueryKernelConfig
                .builder()
                .maxRetainedPartitionSketchBytes(1)
                .maxConcurrentStages(1)
                .pipeline(false)
                .faultTolerance(false)
                .destination(TaskReportMSQDestination.instance())
                .build()
        )
    );
  }

  @Test
  public void test_computeStageGroups_linearWithShuffle()
  {
    final QueryDefinition queryDef = makeLinearQueryDefinitionWithShuffle();

    Assert.assertEquals(
        ImmutableList.of(
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 0),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 1),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 2),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 3)
        ),
        ControllerQueryKernelUtils.computeStageGroups(
            queryDef,
            ControllerQueryKernelConfig
                .builder()
                .maxRetainedPartitionSketchBytes(1)
                .maxConcurrentStages(1)
                .pipeline(false)
                .faultTolerance(false)
                .destination(TaskReportMSQDestination.instance())
                .build()
        )
    );
  }

  @Test
  public void test_computeStageGroups_linearWithoutShuffle_faultTolerant()
  {
    final QueryDefinition queryDef = makeLinearQueryDefinitionWithoutShuffle();

    Assert.assertEquals(
        ImmutableList.of(
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE, 0),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE, 1),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE, 2),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE, 3)
        ),
        ControllerQueryKernelUtils.computeStageGroups(
            queryDef,
            ControllerQueryKernelConfig
                .builder()
                .maxRetainedPartitionSketchBytes(1)
                .maxConcurrentStages(2)
                .pipeline(false)
                .faultTolerance(true)
                .durableStorage(true)
                .destination(TaskReportMSQDestination.instance())
                .build()
        )
    );
  }

  @Test
  public void test_computeStageGroups_linearWithShuffle_faultTolerant()
  {
    final QueryDefinition queryDef = makeLinearQueryDefinitionWithShuffle();

    Assert.assertEquals(
        ImmutableList.of(
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE, 0),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE, 1),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE, 2),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE, 3)
        ),
        ControllerQueryKernelUtils.computeStageGroups(
            queryDef,
            ControllerQueryKernelConfig
                .builder()
                .maxRetainedPartitionSketchBytes(1)
                .maxConcurrentStages(2)
                .pipeline(false)
                .faultTolerance(true)
                .durableStorage(true)
                .destination(TaskReportMSQDestination.instance())
                .build()
        )
    );
  }

  @Test
  public void test_computeStageGroups_linearWithoutShuffle_faultTolerant_durableResults()
  {
    final QueryDefinition queryDef = makeLinearQueryDefinitionWithoutShuffle();

    Assert.assertEquals(
        ImmutableList.of(
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE, 0),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE, 1),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE, 2),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_QUERY_RESULTS, 3)
        ),
        ControllerQueryKernelUtils.computeStageGroups(
            queryDef,
            ControllerQueryKernelConfig
                .builder()
                .maxRetainedPartitionSketchBytes(1)
                .maxConcurrentStages(2)
                .pipeline(false)
                .faultTolerance(true)
                .durableStorage(true)
                .destination(DurableStorageMSQDestination.instance())
                .build()
        )
    );
  }

  @Test
  public void test_computeStageGroups_linearWithShuffle_faultTolerant_durableResults()
  {
    final QueryDefinition queryDef = makeLinearQueryDefinitionWithShuffle();

    Assert.assertEquals(
        ImmutableList.of(
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE, 0),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE, 1),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE, 2),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_QUERY_RESULTS, 3)
        ),
        ControllerQueryKernelUtils.computeStageGroups(
            queryDef,
            ControllerQueryKernelConfig
                .builder()
                .maxRetainedPartitionSketchBytes(1)
                .maxConcurrentStages(2)
                .pipeline(false)
                .faultTolerance(true)
                .durableStorage(true)
                .destination(DurableStorageMSQDestination.instance())
                .build()
        )
    );
  }

  @Test
  public void test_computeStageGroups_linearWithoutShuffle_pipeline_twoAtOnce()
  {
    final QueryDefinition queryDef = makeLinearQueryDefinitionWithoutShuffle();

    Assert.assertEquals(
        // Without a sort-based shuffle, we can't leapfrog, so we launch two groups broken up by LOCAL_STORAGE
        ImmutableList.of(
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 0, 1),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.MEMORY, 2, 3)
        ),
        ControllerQueryKernelUtils.computeStageGroups(
            queryDef,
            ControllerQueryKernelConfig
                .builder()
                .maxRetainedPartitionSketchBytes(1)
                .maxConcurrentStages(2)
                .pipeline(true)
                .faultTolerance(false)
                .destination(TaskReportMSQDestination.instance())
                .build()
        )
    );
  }

  @Test
  public void test_computeStageGroups_linearWithShuffle_pipeline_twoAtOnce()
  {
    final QueryDefinition queryDef = makeLinearQueryDefinitionWithShuffle();

    Assert.assertEquals(
        // With sort-based shuffle, we can leapfrog 4 stages, all of them being in-memory
        ImmutableList.of(
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.MEMORY, 0),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.MEMORY, 1),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.MEMORY, 2),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.MEMORY, 3)
        ),
        ControllerQueryKernelUtils.computeStageGroups(
            queryDef,
            ControllerQueryKernelConfig
                .builder()
                .maxRetainedPartitionSketchBytes(1)
                .maxConcurrentStages(2)
                .pipeline(true)
                .faultTolerance(false)
                .destination(TaskReportMSQDestination.instance())
                .build()
        )
    );
  }

  @Test
  public void test_computeStageGroups_fanIn()
  {
    final QueryDefinition queryDef = makeFanInQueryDefinition();

    Assert.assertEquals(
        ImmutableList.of(
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 0),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 1),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 2),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 3)
        ),
        ControllerQueryKernelUtils.computeStageGroups(
            queryDef,
            ControllerQueryKernelConfig
                .builder()
                .maxRetainedPartitionSketchBytes(1)
                .maxConcurrentStages(1)
                .pipeline(false)
                .faultTolerance(false)
                .destination(TaskReportMSQDestination.instance())
                .build()
        )
    );
  }

  @Test
  public void test_computeStageGroups_fanInWithBroadcast()
  {
    final QueryDefinition queryDef = makeFanInQueryDefinitionWithBroadcast();

    Assert.assertEquals(
        ImmutableList.of(
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 0),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 1),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 2),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 3)
        ),
        ControllerQueryKernelUtils.computeStageGroups(
            queryDef,
            ControllerQueryKernelConfig
                .builder()
                .maxRetainedPartitionSketchBytes(1)
                .maxConcurrentStages(1)
                .pipeline(false)
                .faultTolerance(false)
                .destination(TaskReportMSQDestination.instance())
                .build()
        )
    );
  }

  @Test
  public void test_computeStageGroups_fanIn_faultTolerant()
  {
    final QueryDefinition queryDef = makeFanInQueryDefinition();

    Assert.assertEquals(
        ImmutableList.of(
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE, 0),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE, 1),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE, 2),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE, 3)
        ),
        ControllerQueryKernelUtils.computeStageGroups(
            queryDef,
            ControllerQueryKernelConfig
                .builder()
                .maxRetainedPartitionSketchBytes(1)
                .maxConcurrentStages(2)
                .pipeline(false)
                .faultTolerance(true)
                .durableStorage(true)
                .destination(TaskReportMSQDestination.instance())
                .build()
        )
    );
  }

  @Test
  public void test_computeStageGroups_fanIn_faultTolerant_durableResults()
  {
    final QueryDefinition queryDef = makeFanInQueryDefinition();

    Assert.assertEquals(
        ImmutableList.of(
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE, 0),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE, 1),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE, 2),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.DURABLE_STORAGE_QUERY_RESULTS, 3)
        ),
        ControllerQueryKernelUtils.computeStageGroups(
            queryDef,
            ControllerQueryKernelConfig
                .builder()
                .maxRetainedPartitionSketchBytes(1)
                .maxConcurrentStages(2)
                .pipeline(false)
                .faultTolerance(true)
                .durableStorage(true)
                .destination(DurableStorageMSQDestination.instance())
                .build()
        )
    );
  }

  @Test
  public void test_computeStageGroups_fanIn_pipeline_twoAtOnce()
  {
    final QueryDefinition queryDef = makeFanInQueryDefinition();

    Assert.assertEquals(
        ImmutableList.of(
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 0),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 1),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.MEMORY, 2, 3)
        ),
        ControllerQueryKernelUtils.computeStageGroups(
            queryDef,
            ControllerQueryKernelConfig
                .builder()
                .maxRetainedPartitionSketchBytes(1)
                .maxConcurrentStages(2)
                .pipeline(true)
                .faultTolerance(false)
                .destination(TaskReportMSQDestination.instance())
                .build()
        )
    );
  }

  @Test
  public void test_computeStageGroups_fanInWithBroadcast_pipeline_twoAtOnce()
  {
    final QueryDefinition queryDef = makeFanInQueryDefinitionWithBroadcast();

    Assert.assertEquals(
        // Output of stage 1 is broadcast, so it must run first; then stages 0 and 2 may be launched together
        ImmutableList.of(
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 1),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.LOCAL_STORAGE, 0, 2),
            makeStageGroup(queryDef.getQueryId(), OutputChannelMode.MEMORY, 3)
        ),
        ControllerQueryKernelUtils.computeStageGroups(
            queryDef,
            ControllerQueryKernelConfig
                .builder()
                .maxRetainedPartitionSketchBytes(1)
                .maxConcurrentStages(2)
                .pipeline(true)
                .faultTolerance(false)
                .destination(TaskReportMSQDestination.instance())
                .build()
        )
    );
  }

  private static QueryDefinition makeLinearQueryDefinitionWithShuffle()
  {
    // 0 -> 1 -> 2 -> 3

    return new MockQueryDefinitionBuilder(4)
        .addEdge(0, 1)
        .addEdge(1, 2)
        .addEdge(2, 3)
        .defineStage(0, ShuffleKind.GLOBAL_SORT)
        .defineStage(1, ShuffleKind.GLOBAL_SORT)
        .defineStage(2, ShuffleKind.GLOBAL_SORT)
        .defineStage(3, ShuffleKind.GLOBAL_SORT)
        .getQueryDefinitionBuilder()
        .build();
  }

  private static QueryDefinition makeLinearQueryDefinitionWithoutShuffle()
  {
    // 0 -> 1 -> 2 -> 3

    return new MockQueryDefinitionBuilder(4)
        .addEdge(0, 1)
        .addEdge(1, 2)
        .addEdge(2, 3)
        .getQueryDefinitionBuilder()
        .build();
  }

  private static QueryDefinition makeFanInQueryDefinition()
  {
    // 0 -> 2 -> 3
    //   /
    // 1

    return new MockQueryDefinitionBuilder(4)
        .addEdge(0, 2)
        .addEdge(1, 2)
        .addEdge(2, 3)
        .getQueryDefinitionBuilder()
        .build();
  }

  private static QueryDefinition makeFanInQueryDefinitionWithBroadcast()
  {
    // 0 -> 2 -> 3
    //   / < broadcast
    // 1

    return new MockQueryDefinitionBuilder(4)
        .addEdge(0, 2)
        .addEdge(1, 2, true)
        .addEdge(2, 3)
        .getQueryDefinitionBuilder()
        .build();
  }

  private static QueryDefinition makeMultiProngedQueryDefinition()
  {
    //      0       1
    //      |   /   |
    //      2 /     3
    //      |       |
    //      4       5
    //        \    /
    //          6

    return new MockQueryDefinitionBuilder(7)
        .addEdge(0, 2)
        .addEdge(1, 2)
        .addEdge(1, 3)
        .addEdge(2, 4)
        .addEdge(3, 5)
        .addEdge(4, 6)
        .addEdge(5, 6)
        .getQueryDefinitionBuilder()
        .build();
  }

  public static StageGroup makeStageGroup(
      final String queryId,
      final OutputChannelMode outputChannelMode,
      final int... stageNumbers
  )
  {
    return new StageGroup(
        Arrays.stream(stageNumbers).mapToObj(n -> new StageId(queryId, n)).collect(Collectors.toList()),
        outputChannelMode
    );
  }
}
