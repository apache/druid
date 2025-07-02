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

import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.msq.exec.StageProcessor;
import org.apache.druid.msq.logical.LogicalInputSpec;
import org.apache.druid.msq.logical.StageMaker;
import org.apache.druid.msq.querykit.groupby.GroupByPostShuffleStageProcessor;
import org.apache.druid.msq.querykit.groupby.GroupByPreShuffleStageProcessor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.RowSignature.Finalization;
import org.apache.druid.sql.calcite.aggregation.DimensionExpression;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.DruidNodeStack;
import org.apache.druid.sql.calcite.rel.Grouping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GroupByStages
{
  public static class PreShuffleStage extends ProjectStage
  {
    private GroupByQuery gby;

    public PreShuffleStage(ProjectStage projectStage, GroupByQuery gby)
    {
      super(projectStage, gby.getResultRowSignature(Finalization.NO));
      this.gby = gby;
    }

    @Override
    public LogicalStage extendWith(DruidNodeStack stack)
    {
      return null;
    }

    @Override
    public StageProcessor<?, ?> buildStageProcessor(StageMaker stageMaker)
    {
      return new GroupByPreShuffleStageProcessor(gby);
    }
  }

  static class PostShuffleStage extends AbstractFrameProcessorStage
  {
    private GroupByQuery gby;

    public PostShuffleStage(LogicalStage inputStage, GroupByQuery gby, RowSignature outputSignature)
    {
      super(outputSignature, LogicalInputSpec.of(inputStage));
      this.gby = gby;
    }

    @Override
    public LogicalStage extendWith(DruidNodeStack stack)
    {
      return null;
    }

    @Override
    public StageProcessor<?, ?> buildStageProcessor(StageMaker stageMaker)
    {
      return new GroupByPostShuffleStageProcessor(gby);
    }
  }

  public static LogicalStage buildStages(ProjectStage projectStage, Grouping grouping)
  {
    GroupByQuery gby = makeGbyQuery(projectStage, grouping);
    PreShuffleStage aggStage = new PreShuffleStage(projectStage, gby.withPostAggregatorSpecs(Collections.emptyList()));
    SortStage sortStage = new SortStage(aggStage, getKeyColumns(grouping.getDimensions()));
    PostShuffleStage finalAggStage = new PostShuffleStage(sortStage, gby, grouping.getOutputRowSignature());
    return finalAggStage;
  }

  private static GroupByQuery makeGbyQuery(ProjectStage projectStage, Grouping grouping)
  {
    GroupByQuery.Builder builder = GroupByQuery.builder();
    builder.setDimensions(grouping.getDimensionSpecs());
    builder.setQuerySegmentSpec(QuerySegmentSpec.ETERNITY);
    builder.setGranularity(Granularities.ALL);
    builder.setAggregatorSpecs(grouping.getAggregatorFactories());
    builder.setDimFilter(projectStage.getDimFilter());
    builder.setVirtualColumns(projectStage.getVirtualColumns());
    builder.setPostAggregatorSpecs(grouping.getPostAggregators());
    builder.setDataSource(new TableDataSource("DUMMY"));
    return builder.build();
  }

  private static List<KeyColumn> getKeyColumns(List<DimensionExpression> dimensions)
  {
    List<KeyColumn> columns = new ArrayList<>();
    for (DimensionExpression dimension : dimensions) {
      columns.add(new KeyColumn(dimension.getOutputName(), KeyOrder.ASCENDING));
    }
    return columns;
  }
}
