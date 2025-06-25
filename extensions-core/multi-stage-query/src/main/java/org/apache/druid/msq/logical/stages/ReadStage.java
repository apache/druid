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

import org.apache.druid.msq.exec.StageProcessor;
import org.apache.druid.msq.logical.LogicalInputSpec;
import org.apache.druid.msq.logical.StageMaker;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.DruidNodeStack;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.rel.logical.DruidFilter;
import org.apache.druid.sql.calcite.rel.logical.DruidProject;

/**
 * Represents a stage that reads data from input sources.
 */
public class ReadStage extends AbstractFrameProcessorStage
{
  public ReadStage(RowSignature signature, LogicalInputSpec inputSpec)
  {
    super(signature, inputSpec);
  }

  /**
   * Copy constructor.
   */
  protected ReadStage(ReadStage readStage, RowSignature newSignature)
  {
    super(newSignature, readStage.inputSpecs);
  }

  @Override
  public LogicalStage extendWith(DruidNodeStack stack)
  {
    if (stack.getNode() instanceof DruidFilter) {
      DruidFilter filter = (DruidFilter) stack.getNode();
      return makeFilterStage(stack.getPlannerContext(), filter);
    }

    if (stack.getNode() instanceof DruidProject) {

      DruidProject project = (DruidProject) stack.getNode();
      DruidFilter dummyFilter = new DruidFilter(
          project.getCluster(), project.getTraitSet(), project,
          project.getCluster().getRexBuilder().makeLiteral(true)
      );
      return makeFilterStage(stack.getPlannerContext(), dummyFilter).extendWith(stack);
    }
    return null;
  }

  private LogicalStage makeFilterStage(PlannerContext plannerContext, DruidFilter filter)
  {
    VirtualColumnRegistry virtualColumnRegistry = VirtualColumnRegistry.create(
        signature,
        plannerContext.getExpressionParser(),
        plannerContext.getPlannerConfig().isForceExpressionVirtualColumns()
    );

    DimFilter dimFilter = DruidQuery.getDimFilter(
        plannerContext,
        signature, virtualColumnRegistry, filter
    );

    return new FilterStage(
        this,
        virtualColumnRegistry,
        dimFilter
    );
  }

  @Override
  public StageProcessor<?, ?> buildStageProcessor(StageMaker stageMaker)
  {
    return StageMaker.makeScanStageProcessor(VirtualColumns.EMPTY, signature, null);
  }
}
