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

package org.apache.druid.msq.logical;

import org.apache.druid.error.DruidException;
import org.apache.druid.msq.dart.controller.sql.DartSqlEngine;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.kernel.MixShuffleSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageDefinitionBuilder;
import org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessorFactory;
import org.apache.druid.query.Druids;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.DruidNodeStack;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.Projection;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.rel.logical.DruidFilter;
import org.apache.druid.sql.calcite.rel.logical.DruidProject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class LogicalStageBuilder
{
  private PlannerContext plannerContext;

  public LogicalStageBuilder(PlannerContext plannerContext2)
  {
    this.plannerContext = plannerContext2;
  }

  class StageMaker
  {
    public final AtomicInteger stageIdSeq = new AtomicInteger(0);

    StageDefinition makeScanStage(
        VirtualColumns virtualColumns,
        RowSignature signature,
        List<InputSpec> inputs,
        DimFilter dimFilter)
    {
      ScanQuery s = Druids.newScanQueryBuilder()
          .dataSource(IRRELEVANT)
          .intervals(QuerySegmentSpec.ETERNITY)
          .filters(dimFilter)
          .virtualColumns(virtualColumns)
          .columns(signature.getColumnNames())
          .columnTypes(signature.getColumnTypes())
          .build();
      ScanQueryFrameProcessorFactory scanProcessorFactory = new ScanQueryFrameProcessorFactory(s);
      StageDefinitionBuilder sdb = StageDefinition.builder(stageIdSeq.incrementAndGet())
          .inputs(inputs)
          .signature(signature)
          .shuffleSpec(MixShuffleSpec.instance())
          .processorFactory(scanProcessorFactory);
      return sdb.build(getIdForBuilder());
    }

    private String getIdForBuilder()
    {
      String dartQueryId = plannerContext.queryContext().getString(DartSqlEngine.CTX_DART_QUERY_ID);
      if (dartQueryId != null) {
        return dartQueryId;
      }
      return plannerContext.getSqlQueryId();
    }
  }

  public class AbstractLogicalStage implements LogicalStage
  {
    protected final List<InputSpec> inputSpecs;
    protected final RowSignature signature;
    protected final List<LogicalStage> inputStages;

    public AbstractLogicalStage(RowSignature signature, List<InputSpec> inputs, List<LogicalStage> inputVertices)
    {
      this.inputSpecs = inputs;
      this.signature = signature;
      this.inputStages = inputVertices;
    }

    @Override
    public StageDefinition buildCurrentStage(StageMaker stageMaker)
    {
      throw DruidException.defensive("This should have been implemented - or not reach this point!");
    }

    @Override
    public LogicalStage extendWith(DruidNodeStack stack)
    {
      return null;
    }

    @Override
    public final QueryDefinition build()
    {
      return QueryDefinition.create(buildStageDefinitions(new StageMaker()), plannerContext.queryContext());
    }

    @Override
    public final List<StageDefinition> buildStageDefinitions(StageMaker stageMaker)
    {
      List<StageDefinition> ret = new ArrayList<>();
      if (!inputStages.isEmpty()) {
        throw DruidException.defensive("Not yet supported");
      }
      ret.add(buildCurrentStage(stageMaker));
      return ret;
    }
  }

  public class RootStage extends AbstractLogicalStage
  {
    public RootStage(RowSignature signature, List<InputSpec> inputSpecs)
    {
      super(signature, inputSpecs, Collections.emptyList());
    }

    public RootStage(RootStage root, RowSignature newSignature)
    {
      super(newSignature, root.inputSpecs, root.inputStages);
    }

    @Override
    public StageDefinition buildCurrentStage(StageMaker stageMaker)
    {
      return stageMaker.makeScanStage(VirtualColumns.EMPTY, signature, inputSpecs, null);
    }

    @Override
    public LogicalStage extendWith(DruidNodeStack stack)
    {
      if (stack.peekNode() instanceof DruidFilter) {
        DruidFilter filter = (DruidFilter) stack.peekNode();
        return makeFilterStage(filter);
      }

      if (stack.peekNode() instanceof DruidProject) {

        DruidProject project = (DruidProject) stack.peekNode();
        DruidFilter dummyFilter = new DruidFilter(
            project.getCluster(), project.getTraitSet(), project,
            project.getCluster().getRexBuilder().makeLiteral(true)
        );
        return makeFilterStage(dummyFilter).extendWith(stack);
      }
      return null;
    }

    private LogicalStage makeFilterStage(DruidFilter filter)
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
  }

  public FilterStage create(RootStage inputStage, DruidFilter filter)
  {
    VirtualColumnRegistry virtualColumnRegistry = VirtualColumnRegistry.create(
        inputStage.signature,
        plannerContext.getExpressionParser(),
        plannerContext.getPlannerConfig().isForceExpressionVirtualColumns()
    );
    DimFilter dimFilter = DruidQuery.getDimFilter(plannerContext, inputStage.signature, virtualColumnRegistry, filter);
    return new FilterStage(inputStage, virtualColumnRegistry, dimFilter);
  }

  class FilterStage extends RootStage
  {
    protected final VirtualColumnRegistry virtualColumnRegistry;
    private DimFilter dimFilter;

    public FilterStage(RootStage inputStage, VirtualColumnRegistry virtualColumnRegistry, DimFilter dimFilter)
    {
      super(inputStage, inputStage.signature);
      this.virtualColumnRegistry = virtualColumnRegistry;
      this.dimFilter = dimFilter;
    }

    public FilterStage(FilterStage root, VirtualColumnRegistry newVirtualColumnRegistry, RowSignature rowSignature)
    {
      super(root, rowSignature);
      this.dimFilter = root.dimFilter;
      this.virtualColumnRegistry = newVirtualColumnRegistry;
    }

    @Override
    public StageDefinition buildCurrentStage(StageMaker stageMaker)
    {
      VirtualColumns output = virtualColumnRegistry.build(Collections.emptySet());
      return stageMaker.makeScanStage(output, signature, inputSpecs, dimFilter);
    }

    @Override
    public LogicalStage extendWith(DruidNodeStack stack)
    {
      if (stack.peekNode() instanceof DruidProject) {
        DruidProject project = (DruidProject) stack.peekNode();
        Projection preAggregation = Projection
            .preAggregation(project, plannerContext, signature, virtualColumnRegistry);

        return new ProjectStage(
            this,
            virtualColumnRegistry,
            preAggregation.getOutputRowSignature()
        );
      }
      return null;
    }
  }

  class ProjectStage extends FilterStage
  {
    public ProjectStage(FilterStage root, VirtualColumnRegistry newVirtualColumnRegistry, RowSignature rowSignature)
    {
      super(root, newVirtualColumnRegistry, rowSignature);
    }

    @Override
    public LogicalStage extendWith(DruidNodeStack stack)
    {
      return null;
    }
  }

  private static final String IRRELEVANT = "irrelevant";

  public RootStage makeRootStage(RowSignature rowSignature, List<InputSpec> isp)
  {
    return new RootStage(rowSignature, isp);
  }

}
