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

package org.apache.druid.msq.kernel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.msq.exec.Limits;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.input.table.TableInputSpec;
import org.apache.druid.msq.querykit.common.OffsetLimitStageProcessor;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.UUID;

public class QueryDefinitionTest
{
  @Test
  public void testSerde() throws Exception
  {
    final QueryDefinition queryDef =
        QueryDefinition
            .builder(UUID.randomUUID().toString())
            .add(
                StageDefinition
                    .builder(0)
                    .processor(new OffsetLimitStageProcessor(0, 1L))
                    .shuffleSpec(
                        new GlobalSortMaxCountShuffleSpec(
                            new ClusterBy(ImmutableList.of(new KeyColumn("s", KeyOrder.ASCENDING)), 0),
                            2,
                            false,
                            ShuffleSpec.UNLIMITED
                        )
                    )
                    .maxWorkerCount(3)
                    .signature(RowSignature.builder().add("s", ColumnType.STRING).build())
            )
            .build();

    final ObjectMapper mapper = TestHelper.makeJsonMapper()
                                          .registerModules(new MSQIndexingModule().getJacksonModules());

    Assertions.assertEquals(
        queryDef,
        mapper.readValue(mapper.writeValueAsString(queryDef), QueryDefinition.class)
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(QueryDefinition.class)
                  .withNonnullFields("stageDefinitions", "finalStage")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testWithRuntimeBoundsSizesPartitionsByReader()
  {
    // Stages 0 and 2 are co-partitioned inputs of stage 3. Stage 0 is a leaf stage and stage 2 is not.
    final QueryDefinition queryDef =
        QueryDefinition
            .builder(UUID.randomUUID().toString())
            .add(adjustableStage(0).inputs(new TableInputSpec("foo", null, null)))
            .add(adjustableStage(1).inputs(new TableInputSpec("bar", null, null)))
            .add(adjustableStage(2).inputs(new StageInputSpec(1)))
            .add(stage(3).inputs(new StageInputSpec(0), new StageInputSpec(2)))
            .build()
            .withRuntimeBounds(10, 2, 3);

    Assertions.assertEquals(10, queryDef.getStageDefinition(0).getMaxWorkerCount());
    Assertions.assertEquals(10, queryDef.getStageDefinition(1).getMaxWorkerCount());
    Assertions.assertEquals(2, queryDef.getStageDefinition(2).getMaxWorkerCount());
    Assertions.assertEquals(2, queryDef.getStageDefinition(3).getMaxWorkerCount());

    Assertions.assertEquals(6, queryDef.getStageDefinition(0).getShuffleSpec().partitionCount());
    Assertions.assertEquals(6, queryDef.getStageDefinition(1).getShuffleSpec().partitionCount());
    Assertions.assertEquals(6, queryDef.getStageDefinition(2).getShuffleSpec().partitionCount());
    Assertions.assertFalse(queryDef.getStageDefinition(0).getShuffleSpec().isAdjustable());
  }

  @Test
  public void testWithRuntimeBoundsNoNonBroadcastReader()
  {
    // Stage 0 is read only as a broadcast input, and stage 1 is the final stage, so both use their own worker counts.
    final QueryDefinition queryDef =
        QueryDefinition
            .builder(UUID.randomUUID().toString())
            .add(adjustableStage(0).inputs(new TableInputSpec("foo", null, null)))
            .add(
                adjustableStage(1)
                    .inputs(new TableInputSpec("bar", null, null), new StageInputSpec(0))
                    .broadcastInputs(Set.of(1))
            )
            .build()
            .withRuntimeBounds(10, 2, 3);

    Assertions.assertEquals(30, queryDef.getStageDefinition(0).getShuffleSpec().partitionCount());
    Assertions.assertEquals(30, queryDef.getStageDefinition(1).getShuffleSpec().partitionCount());
  }

  private static StageDefinitionBuilder adjustableStage(final int stageNumber)
  {
    return stage(stageNumber).shuffleSpec(
        new HashShuffleSpec(new ClusterBy(ImmutableList.of(new KeyColumn("s", KeyOrder.ASCENDING)), 0), 1, true)
    );
  }

  private static StageDefinitionBuilder stage(final int stageNumber)
  {
    return StageDefinition
        .builder(stageNumber)
        .processor(new OffsetLimitStageProcessor(0, 1L))
        .maxWorkerCount(Limits.MAX_WORKERS)
        .signature(RowSignature.builder().add("s", ColumnType.STRING).build());
  }
}
