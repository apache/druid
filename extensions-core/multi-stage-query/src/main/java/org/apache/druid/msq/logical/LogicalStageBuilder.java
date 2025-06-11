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

import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.msq.kernel.ShuffleSpec;
import org.apache.druid.msq.querykit.BaseFrameProcessorFactory;
import org.apache.druid.msq.querykit.QueryKitUtils;
import org.apache.druid.msq.querykit.ShuffleSpecFactories;
import org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessorFactory;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.DruidNodeStack;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.Projection;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.rel.logical.DruidFilter;
import org.apache.druid.sql.calcite.rel.logical.DruidProject;
import java.util.Collections;
import java.util.List;

/**
 * Helper class to build a {@link LogicalStage} tree.
 *
 * Tightly coupled to {@link DruidLogicalToQueryDefinitionTranslator}. Currently
 * its just a context to hold all the {@link LogicalStage} classes.
 *
 * FIXME: this is not a builder!
 */
public class LogicalStageBuilder
{
  private PlannerContext plannerContext;

  public LogicalStageBuilder(PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;
  }

  public abstract static class AbstractLogicalStage implements LogicalStage
  {
    protected final List<LogicalInputSpec> inputSpecs;
    protected final RowSignature signature;

    public AbstractLogicalStage(RowSignature signature, LogicalInputSpec input)
    {
      this(signature, Collections.singletonList(input));
      // getNextStageId();
    }

    public AbstractLogicalStage(RowSignature signature, List<LogicalInputSpec> inputs)
    {
      this.inputSpecs = inputs;
      this.signature = signature;
    }

    @Override
    public RowSignature getLogicalRowSignature()
    {
      return signature;
    }

    public RowSignature getSignature()
    {
      return signature;
    }
  }

  public abstract class AbstractFrameProcessorStage extends AbstractLogicalStage
  {
    public AbstractFrameProcessorStage(RowSignature signature, LogicalInputSpec input)
    {
      super(signature, input);
    }

    protected abstract BaseFrameProcessorFactory buildFrameProcessor(StageMaker stageMaker);

    public AbstractFrameProcessorStage(RowSignature signature, List<LogicalInputSpec> input)
    {
      super(signature, input);
    }
  }

  public abstract class AbstractShuffleStage extends AbstractLogicalStage
  {
    public AbstractShuffleStage(RowSignature signature, LogicalInputSpec input)
    {
      super(signature, input);
    }

    protected abstract ShuffleSpec buildShuffleSpec();

    public AbstractShuffleStage(RowSignature signature, List<LogicalInputSpec> input)
    {
      super(signature, input);
    }
  }

  /**
   * Represents a stage that reads data from input sources.
   */
  public class ReadStage extends AbstractFrameProcessorStage
  {
    public ReadStage(RowSignature signature, LogicalInputSpec inputSpecs)
    {
      super(signature, inputSpecs);
    }

    /**
     * Copy constructor.
     */
    public ReadStage(ReadStage readStage, RowSignature newSignature)
    {
      super(newSignature, readStage.inputSpecs);
    }

    @Override
    public LogicalStage extendWith(DruidNodeStack stack)
    {
      if (stack.getNode() instanceof DruidFilter) {
        DruidFilter filter = (DruidFilter) stack.getNode();
        return makeFilterStage(filter);
      }

      if (stack.getNode() instanceof DruidProject) {

        DruidProject project = (DruidProject) stack.getNode();
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

    @Override
    protected BaseFrameProcessorFactory buildFrameProcessor(StageMaker stageMaker)
    {
      ScanQueryFrameProcessorFactory scanFrameProcessor = stageMaker
          .makeScanFrameProcessor(VirtualColumns.EMPTY, signature, null);
      return scanFrameProcessor;
    }
  }

  public FilterStage create(ReadStage inputStage, DruidFilter filter)
  {
    VirtualColumnRegistry virtualColumnRegistry = VirtualColumnRegistry.create(
        inputStage.signature,
        plannerContext.getExpressionParser(),
        plannerContext.getPlannerConfig().isForceExpressionVirtualColumns()
    );
    DimFilter dimFilter = DruidQuery.getDimFilter(plannerContext, inputStage.signature, virtualColumnRegistry, filter);
    return new FilterStage(inputStage, virtualColumnRegistry, dimFilter);
  }

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
    public FilterStage(FilterStage stage, VirtualColumnRegistry newVirtualColumnRegistry, RowSignature rowSignature)
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
        Projection projection = Projection.preAggregation(project, plannerContext, signature, virtualColumnRegistry);

        return new ProjectStage(
            this,
            virtualColumnRegistry,
            projection.getOutputRowSignature()
        );
      }
      return null;
    }

    @Override
    protected BaseFrameProcessorFactory buildFrameProcessor(StageMaker stageMaker)
    {
      ScanQueryFrameProcessorFactory scanFrameProcessor = stageMaker
          .makeScanFrameProcessor(virtualColumnRegistry.build(Collections.emptySet()), signature, dimFilter);
      return scanFrameProcessor;
    }

  }

  class ProjectStage extends FilterStage
  {
    public ProjectStage(FilterStage root, VirtualColumnRegistry newVirtualColumnRegistry, RowSignature rowSignature)
    {
      super(root, newVirtualColumnRegistry, rowSignature);
    }

    public ProjectStage(ProjectStage root, RowSignature rowSignature)
    {
      super(root, root.virtualColumnRegistry, rowSignature);
    }

    @Override
    public LogicalStage extendWith(DruidNodeStack stack)
    {
      return null;
    }
  }

  class SortStage extends AbstractShuffleStage
  {
    protected List<KeyColumn> keyColumns;
    // FIXME: remove
    private LogicalStage inputStage;

    public SortStage(LogicalStage inputStage, List<KeyColumn> keyColumns)
    {
      super(
          QueryKitUtils.sortableSignature(inputStage.getLogicalRowSignature(), keyColumns),
          LogicalInputSpec.of(inputStage)
      );
      this.inputStage = inputStage;
      this.keyColumns = keyColumns;
    }

    @Override
    public LogicalStage extendWith(DruidNodeStack stack)
    {
      return null;
    }

    @Override
    public RowSignature getLogicalRowSignature()
    {
      return inputStage.getLogicalRowSignature();
    }

    @Override
    protected ShuffleSpec buildShuffleSpec()
    {
      final Granularity segmentGranularity = Granularities.ALL;
      // FIXME:
      // QueryKitUtils.getSegmentGranularityFromContext(jsonMapper,
      // queryToRun.getContext());

      final ClusterBy clusterBy = QueryKitUtils
          .clusterByWithSegmentGranularity(new ClusterBy(keyColumns, 0), segmentGranularity);
      // FIXME targetSize == 1
      return ShuffleSpecFactories.globalSortWithMaxPartitionCount(1).build(clusterBy, false);
    }
  }

  public ReadStage makeReadStage(RowSignature rowSignature, LogicalInputSpec isp)
  {
    return new ReadStage(rowSignature, isp);
  }

  public LogicalStage makeSortStage(LogicalStage inputStage, List<KeyColumn> keyColumns)
  {
    return new SortStage(inputStage, keyColumns);
  }

}
