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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.querykit.common.OffsetLimitStageProcessor;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollectorImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Test;

public class StageDefinitionTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(StageDefinition.class)
                  .withIgnoredFields("frameReader")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testGeneratePartitionsForNullShuffle()
  {
    StageDefinition stageDefinition = new StageDefinition(
        new StageId("query", 1),
        ImmutableList.of(new StageInputSpec(0)),
        ImmutableSet.of(),
        new OffsetLimitStageProcessor(0, 1L),
        RowSignature.empty(),
        null,
        0,
        false
    );

    Assert.assertThrows(ISE.class, () -> stageDefinition.generatePartitionBoundariesForShuffle(null));
  }

  @Test
  public void testGeneratePartitionsForNonNullShuffleWithNullCollector()
  {
    StageDefinition stageDefinition = new StageDefinition(
        new StageId("query", 1),
        ImmutableList.of(new StageInputSpec(0)),
        ImmutableSet.of(),
        new OffsetLimitStageProcessor(0, 1L),
        RowSignature.empty(),
        new GlobalSortMaxCountShuffleSpec(
            new ClusterBy(ImmutableList.of(new KeyColumn("test", KeyOrder.ASCENDING)), 0),
            2,
            false,
            ShuffleSpec.UNLIMITED
        ),
        1,
        false
    );

    Assert.assertThrows(ISE.class, () -> stageDefinition.generatePartitionBoundariesForShuffle(null));
  }

  @Test
  public void testGeneratePartitionsForNonNullShuffleWithNonNullCollector()
  {
    StageDefinition stageDefinition = new StageDefinition(
        new StageId("query", 1),
        ImmutableList.of(new StageInputSpec(0)),
        ImmutableSet.of(),
        new OffsetLimitStageProcessor(0, 1L),
        RowSignature.empty(),
        new GlobalSortMaxCountShuffleSpec(
            new ClusterBy(ImmutableList.of(new KeyColumn("test", KeyOrder.ASCENDING)), 0),
            1,
            false,
            ShuffleSpec.UNLIMITED
        ),
        1,
        false
    );

    Assert.assertThrows(
        ISE.class,
        () -> stageDefinition.generatePartitionBoundariesForShuffle(ClusterByStatisticsCollectorImpl.create(new ClusterBy(
            ImmutableList.of(new KeyColumn("test", KeyOrder.ASCENDING)),
            1
        ), RowSignature.builder().add("test", ColumnType.STRING).build(), 1000, 100, false, false))
    );
  }
}
