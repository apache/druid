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

import com.google.common.collect.Lists;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.kernel.FrameProcessorFactory;
import org.apache.druid.msq.kernel.MixShuffleSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.ShuffleSpec;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageDefinitionBuilder;
import org.apache.druid.msq.querykit.QueryKitUtils;
import org.apache.druid.msq.querykit.ShuffleSpecFactories;
import org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessorFactory;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
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
import org.apache.druid.sql.calcite.rel.logical.DruidSort;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;

/**
 * Helper class to build a {@link LogicalStage} tree.
 *
 * Tightly coupled to {@link DruidLogicalToQueryDefinitionTranslator}. Currently
 * its just a context to hold all the {@link LogicalStage} classes.
 */
public class LogicalStageBuilder
{
  private PlannerContext plannerContext;

  public LogicalStageBuilder(PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;
  }

  public interface DagStage
  {
    StageDefinitionBuilder buildStages(StageMaker stageMaker);
  }

  public static class FrameProcessorStage implements DagStage
  {
    private final List<InputSpec> inputs;
    private final FrameProcessorFactory<?,?,?> scanProcessorFactory;
    private RowSignature signature;

    public FrameProcessorStage(
        List<InputSpec> inputs,
        RowSignature signature,
        FrameProcessorFactory<?,?,?> scanProcessorFactory)
    {
      this.inputs = inputs;
      this.signature = signature;
      this.scanProcessorFactory = scanProcessorFactory;
    }

    @Override
    public StageDefinitionBuilder buildStages(StageMaker stageMaker)
    {
      StageDefinitionBuilder sdb = StageDefinition.builder(stageMaker.getNextStageId())
          .inputs(inputs)
          .processorFactory(scanProcessorFactory)
          .signature(signature);

      return sdb;

    }
  }

  public static class ShuffleStage implements DagStage
  {

    private DagStage inputStage;
    private RowSignature signature;
    private List<KeyColumn> keyColumns;

    public ShuffleStage(DagStage inputStage, RowSignature signature, List<KeyColumn> keyColumns)
    {
      this.inputStage = inputStage;
      this.signature = signature;
      this.keyColumns = keyColumns;
    }

    @Override
    public StageDefinitionBuilder buildStages(StageMaker stageMaker)
    {
      StageDefinitionBuilder sdb = inputStage.buildStages(stageMaker);
      sdb.shuffleSpec(stageMaker.shuffleFor(keyColumns));
      return sdb;
    }

  }

  class StageMaker
  {
    /** Provides ids for the stages. */
    private int stageIdSeq = 0;

    StageDefinition makeScanStage(
        VirtualColumns virtualColumns,
        RowSignature signature,
        List<InputSpec> inputs,
        DimFilter dimFilter,
        List<KeyColumn> keyColumns)
    {
      ScanQueryFrameProcessorFactory scanProcessorFactory = makeScanFrameProcessor(
          virtualColumns, signature, dimFilter
      );
      StageDefinitionBuilder sdb = StageDefinition.builder(getNextStageId())
          .inputs(inputs)
          .processorFactory(scanProcessorFactory)
          .signature(signature)
          .shuffleSpec(shuffleFor(keyColumns));

      return sdb.build(getIdForBuilder());
    }

    StageDefinition makeScanStage2(
        VirtualColumns virtualColumns,
        RowSignature signature,
        List<InputSpec> inputs,
        DimFilter dimFilter)
    {
      ScanQueryFrameProcessorFactory scanProcessorFactory = makeScanFrameProcessor(
          virtualColumns, signature, dimFilter
      );
      StageDefinitionBuilder sdb = StageDefinition.builder(getNextStageId())
          .inputs(inputs)
          .processorFactory(scanProcessorFactory)
          .signature(signature);

      return sdb.build(getIdForBuilder());
    }

    Stack<DagStage> stack = new Stack<>();

    public void pushFrameProcessorStage(
        List<InputSpec> inputs,
        RowSignature signature,
        FrameProcessorFactory<?, ?, ?> processorFactory)
    {
      stack.push(new FrameProcessorStage(inputs, signature, processorFactory));
    }

    public void pushSortStage(RowSignature signature, List<KeyColumn> keyColumns)
    {
      stack.push(new ShuffleStage(stack.pop(), signature, keyColumns));
    }

    public ScanQueryFrameProcessorFactory makeScanFrameProcessor(
        VirtualColumns virtualColumns,
        RowSignature signature,
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

      return new ScanQueryFrameProcessorFactory(s);
    }

    private ShuffleSpec shuffleFor(List<KeyColumn> keyColumns)
    {
      if (keyColumns == null) {
        return MixShuffleSpec.instance();
      } else {

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


    StageDefinition build1() {
      DagStage node = stack.pop();
      return node.buildStages(new StageMaker()).build(getIdForBuilder());
    }

    private int getNextStageId()
    {
      return stageIdSeq++;
    }

    private String getIdForBuilder()
    {
      String dartQueryId = plannerContext.queryContext().getString(QueryContexts.CTX_DART_QUERY_ID);
      if (dartQueryId != null) {
        return dartQueryId;
      }
      return plannerContext.getSqlQueryId();
    }
  }

  public abstract class AbstractLogicalStage implements LogicalStage
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
      buildCurrentStage(stageMaker);
      ret.add(stageMaker.build1());
      return ret;
    }
  }

  /**
   * Represents a stage that reads data from input sources.
   */
  public class ReadStage extends AbstractLogicalStage
  {
    public ReadStage(RowSignature signature, List<InputSpec> inputSpecs)
    {
      super(signature, inputSpecs, Collections.emptyList());
    }

    /**
     * Copy constructor.
     */
    public ReadStage(ReadStage readStage, RowSignature newSignature)
    {
      super(newSignature, readStage.inputSpecs, readStage.inputStages);
    }

    @Override
    public StageDefinition buildCurrentStage(StageMaker stageMaker)
    {
      ScanQueryFrameProcessorFactory scanFrameProcessor = stageMaker
          .makeScanFrameProcessor(VirtualColumns.EMPTY, signature, null);
      stageMaker.pushFrameProcessorStage(inputSpecs, signature, scanFrameProcessor);
      stageMaker.pushSortStage(signature, Collections.emptyList());
      return null;
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
    public StageDefinition buildCurrentStage(StageMaker stageMaker)
    {
      VirtualColumns output = virtualColumnRegistry.build(Collections.emptySet());
      ScanQueryFrameProcessorFactory scanFrameProcessor = stageMaker
          .makeScanFrameProcessor(output, signature, dimFilter);
      stageMaker.pushFrameProcessorStage(inputSpecs, signature, scanFrameProcessor);
      stageMaker.pushSortStage(signature, Collections.emptyList());
      return null;
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
      if (stack.getNode() instanceof DruidSort) {
        DruidSort sort = (DruidSort) stack.getNode();
        if (sort.hasLimitOrOffset()) {
          throw DruidException.defensive("Sort with limit or offset is not supported in MSQ logical stage builder");
        }
        List<OrderByColumnSpec> orderBySpecs = DruidQuery.buildOrderByColumnSpecs(signature, sort);
        List<KeyColumn> keyColumns = Lists.transform(orderBySpecs, KeyColumn::fromOrderByColumnSpec);
        return new SortStage(this, keyColumns);
      }
      return null;
    }
  }

  class SortStage extends ProjectStage
  {
    private List<KeyColumn> keyColumns;

    public SortStage(ProjectStage root, List<KeyColumn> keyColumns)
    {
      super(root, QueryKitUtils.sortableSignature(root.signature, keyColumns));
      this.keyColumns = keyColumns;
    }

    @Override
    public LogicalStage extendWith(DruidNodeStack stack)
    {
      return null;
    }

    @Override
    public StageDefinition buildCurrentStage(StageMaker stageMaker)
    {
      super.buildCurrentStage(stageMaker);
      stageMaker.pushSortStage(signature, keyColumns);
      return null;
    }
  }

  private static final String IRRELEVANT = "irrelevant";

  public ReadStage makeReadStage(RowSignature rowSignature, List<InputSpec> isp)
  {
    return new ReadStage(rowSignature, isp);
  }

}
