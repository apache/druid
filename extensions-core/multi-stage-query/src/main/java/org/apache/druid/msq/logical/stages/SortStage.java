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

package org.apache.druid.msq.logical.stages;

import com.google.common.collect.ImmutableList;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.msq.exec.StageProcessor;
import org.apache.druid.msq.kernel.ShuffleSpec;
import org.apache.druid.msq.logical.LogicalInputSpec;
import org.apache.druid.msq.logical.StageMaker;
import org.apache.druid.msq.querykit.QueryKitUtils;
import org.apache.druid.msq.querykit.ShuffleSpecFactories;
import org.apache.druid.msq.querykit.common.OffsetLimitStageProcessor;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.OffsetLimit;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.DruidNodeStack;

import java.util.List;

public class SortStage extends AbstractShuffleStage
{
  protected final List<KeyColumn> keyColumns;

  public SortStage(LogicalStage inputStage, List<KeyColumn> keyColumns)
  {
    super(
        QueryKitUtils.sortableSignature(inputStage.getLogicalRowSignature(), keyColumns),
        LogicalInputSpec.of(inputStage)
    );
    this.keyColumns = keyColumns;
  }

  @Override
  public RowSignature getLogicalRowSignature()
  {
    return inputSpecs.get(0).getRowSignature();
  }

  @Override
  public ShuffleSpec buildShuffleSpec()
  {
    final ClusterBy clusterBy = new ClusterBy(keyColumns, 0);
    return ShuffleSpecFactories.globalSortWithMaxPartitionCount(1).build(clusterBy, false);
  }

  @Override
  public LogicalStage extendWith(DruidNodeStack stack)
  {
    return null;
  }


  public static class OffsetLimitStage extends AbstractFrameProcessorStage
  {
    protected final OffsetLimit offsetLimit;

    public OffsetLimitStage(LogicalStage inputStage, OffsetLimit offsetLimit)
    {
      super(inputStage.getRowSignature(), ImmutableList.of(LogicalInputSpec.of(inputStage)));
      this.offsetLimit = offsetLimit;
    }

    @Override
    public LogicalStage extendWith(DruidNodeStack stack)
    {
      return null;
    }

    @Override
    public StageProcessor<?, ?> buildStageProcessor(StageMaker stageMaker)
    {
      return new OffsetLimitStageProcessor(
          offsetLimit.getOffset(),
          offsetLimit.hasLimit() ? offsetLimit.getLimit() : null
      );
    }

    @Override
    public RowSignature getLogicalRowSignature()
    {
      return inputSpecs.get(0).getRowSignature();
    }

  }
}
