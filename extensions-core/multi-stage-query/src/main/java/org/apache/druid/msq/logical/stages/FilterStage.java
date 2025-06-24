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
import org.apache.druid.msq.logical.StageMaker;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.DruidNodeStack;
import org.apache.druid.sql.calcite.rel.Projection;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.rel.logical.DruidProject;

import java.util.Collections;

class FilterStage extends ReadStage
{
  protected final VirtualColumnRegistry virtualColumnRegistry;
  protected final DimFilter dimFilter;

  public FilterStage(ReadStage inputStage, VirtualColumnRegistry virtualColumnRegistry, DimFilter dimFilter)
  {
    super(inputStage, inputStage.signature);
    this.virtualColumnRegistry = virtualColumnRegistry;
    this.dimFilter = dimFilter;
  }

  /**
   * Copy constructor.
   */
  protected FilterStage(FilterStage stage, VirtualColumnRegistry newVirtualColumnRegistry, RowSignature rowSignature)
  {
    super(stage, rowSignature);
    this.dimFilter = stage.dimFilter;
    this.virtualColumnRegistry = newVirtualColumnRegistry;
  }

  @Override
  public LogicalStage extendWith(DruidNodeStack stack)
  {
    if (stack.getNode() instanceof DruidProject) {
      DruidProject project = (DruidProject) stack.getNode();
      Projection projection = Projection.preAggregation(project, stack.getPlannerContext(), signature, virtualColumnRegistry);

      return new ProjectStage(
          this,
          virtualColumnRegistry,
          projection.getOutputRowSignature()
      );
    }
    return null;
  }

  @Override
  public StageProcessor<?, ?> buildStageProcessor(StageMaker stageMaker)
  {
    VirtualColumns virtualColumns = virtualColumnRegistry.build(Collections.emptySet());
    return StageMaker.makeScanStageProcessor(virtualColumns, signature, dimFilter);
  }
}
