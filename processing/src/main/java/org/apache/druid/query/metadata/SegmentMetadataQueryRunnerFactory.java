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

package org.apache.druid.query.metadata;

import com.google.inject.Inject;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.ChainedExecutionQueryRunner;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.ColumnIncluderator;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.segment.AggregateProjectionMetadata;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.PhysicalSegmentInspector;
import org.apache.druid.segment.Segment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class SegmentMetadataQueryRunnerFactory implements QueryRunnerFactory<SegmentAnalysis, SegmentMetadataQuery>
{
  private final SegmentMetadataQueryQueryToolChest toolChest;
  private final QueryWatcher queryWatcher;

  @Inject
  public SegmentMetadataQueryRunnerFactory(
      SegmentMetadataQueryQueryToolChest toolChest,
      QueryWatcher queryWatcher
  )
  {
    this.toolChest = toolChest;
    this.queryWatcher = queryWatcher;
  }

  @Override
  public QueryRunner<SegmentAnalysis> createRunner(final Segment segment)
  {
    return new QueryRunner<>()
    {
      @Override
      public Sequence<SegmentAnalysis> run(QueryPlus<SegmentAnalysis> inQ, ResponseContext responseContext)
      {
        SegmentMetadataQuery updatedQuery = ((SegmentMetadataQuery) inQ.getQuery())
            .withFinalizedAnalysisTypes(toolChest.getConfig());
        final SegmentAnalyzer analyzer = new SegmentAnalyzer(updatedQuery.getAnalysisTypes());
        final Map<String, ColumnAnalysis> analyzedColumns = analyzer.analyze(segment);
        final long numRows = analyzer.numRows(segment);
        long totalSize = 0;

        if (analyzer.analyzingSize()) {
          // Initialize with the size of the whitespace, 1 byte per
          totalSize = analyzedColumns.size() * numRows;
        }

        LinkedHashMap<String, ColumnAnalysis> columns = new LinkedHashMap<>();
        ColumnIncluderator includerator = updatedQuery.getToInclude();
        for (Map.Entry<String, ColumnAnalysis> entry : analyzedColumns.entrySet()) {
          final String columnName = entry.getKey();
          final ColumnAnalysis column = entry.getValue();

          if (!column.isError()) {
            totalSize += column.getSize();
          }
          if (includerator.include(columnName)) {
            columns.put(columnName, column);
          }
        }
        List<Interval> retIntervals = updatedQuery.analyzingInterval() ?
                                      Collections.singletonList(segment.getDataInterval()) : null;

        final Map<String, AggregatorFactory> aggregators;
        Metadata metadata = null;
        if (updatedQuery.hasAggregators()) {
          metadata = getMetadata(segment);
          if (metadata != null && metadata.getAggregators() != null) {
            aggregators = new HashMap<>();
            for (AggregatorFactory aggregator : metadata.getAggregators()) {
              aggregators.put(aggregator.getName(), aggregator);
            }
          } else {
            aggregators = null;
          }
        } else {
          aggregators = null;
        }

        final Map<String, AggregateProjectionMetadata> projectionsMap;
        if (updatedQuery.hasProjections()
            && ((metadata = Objects.isNull(metadata) ? getMetadata(segment) : metadata)) != null
            && metadata.getProjections() != null) {
          projectionsMap = metadata.getProjections()
                                   .stream()
                                   .collect(Collectors.toUnmodifiableMap(
                                       projectionMetadata -> projectionMetadata.getSchema().getName(),
                                       p -> p
                                   ));
        } else {
          projectionsMap = null;
        }

        final TimestampSpec timestampSpec;
        if (updatedQuery.hasTimestampSpec()) {
          if (metadata == null) {
            metadata = getMetadata(segment);
          }
          timestampSpec = metadata != null ? metadata.getTimestampSpec() : null;
        } else {
          timestampSpec = null;
        }

        final Granularity queryGranularity;
        if (updatedQuery.hasQueryGranularity()) {
          if (metadata == null) {
            metadata = getMetadata(segment);
          }
          queryGranularity = metadata != null ? metadata.getQueryGranularity() : null;
        } else {
          queryGranularity = null;
        }

        Boolean rollup = null;
        if (updatedQuery.hasRollup()) {
          if (metadata == null) {
            metadata = getMetadata(segment);
          }
          rollup = metadata != null ? metadata.isRollup() : null;
          if (rollup == null) {
            // in this case, this segment is built before no-rollup function is coded,
            // thus it is built with rollup
            rollup = Boolean.TRUE;
          }
        }

        return Sequences.simple(
            Collections.singletonList(
                new SegmentAnalysis(
                    segment.getId().toString(),
                    retIntervals,
                    columns,
                    totalSize,
                    numRows,
                    aggregators,
                    projectionsMap,
                    timestampSpec,
                    queryGranularity,
                    rollup
                )
            )
        );
      }
    };
  }

  @Override
  public QueryRunner<SegmentAnalysis> mergeRunners(
      QueryProcessingPool queryProcessingPool,
      Iterable<QueryRunner<SegmentAnalysis>> queryRunners
  )
  {
    return new ChainedExecutionQueryRunner<>(queryProcessingPool, queryWatcher, queryRunners);
  }

  @Override
  public QueryToolChest<SegmentAnalysis, SegmentMetadataQuery> getToolchest()
  {
    return toolChest;
  }

  @Nullable
  private Metadata getMetadata(Segment segment)
  {
    PhysicalSegmentInspector inspector = segment.as(PhysicalSegmentInspector.class);
    if (inspector != null) {
      return inspector.getMetadata();
    }
    return null;
  }
}
