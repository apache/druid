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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis.ContainerAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.List;

/**
 * Verifies {@link SegmentMetadataQuery.AnalysisType#CONTAINERSIZE} against a real V10-format segment: containers are
 * only populated when the analysis type is requested, and their bundle names line up with the base table and
 * projection names used at write time.
 */
public class SegmentMetadataQueryContainerSizeTest
{
  private static final String PROJECTION_NAME = "dim_sum";
  private static final String DATASOURCE = "containerSizeTestDatasource";

  private static final SegmentMetadataQueryRunnerFactory FACTORY = new SegmentMetadataQueryRunnerFactory(
      new SegmentMetadataQueryQueryToolChest(new SegmentMetadataQueryConfig()),
      QueryRunnerTestHelper.NOOP_QUERYWATCHER
  );

  @Test
  public void testContainerSizeAnalysis()
  {
    final AggregateProjectionSpec projectionSpec =
        AggregateProjectionSpec.builder(PROJECTION_NAME)
                                .groupingColumns(new StringDimensionSchema("dim"))
                                .aggregators(new LongSumAggregatorFactory("m_sum", "m_sum"))
                                .build();

    final IncrementalIndexSchema schema =
        IncrementalIndexSchema.builder()
                              .withDimensionsSpec(new DimensionsSpec(List.of(new StringDimensionSchema("dim"))))
                              .withMetrics(new LongSumAggregatorFactory("m_sum", "m"))
                              .withRollup(false)
                              .withMinTimestamp(DateTimes.of("2013-01-01").getMillis())
                              .withProjections(List.of(projectionSpec))
                              .build();

    final List<InputRow> rows = List.of(
        new MapBasedInputRow(DateTimes.of("2013-01-01"), List.of("dim"), ImmutableMap.of("dim", "a", "m", 1L)),
        new MapBasedInputRow(DateTimes.of("2013-01-01"), List.of("dim"), ImmutableMap.of("dim", "b", "m", 2L))
    );

    final File tmpDir = FileUtils.createTempDir();
    final QueryableIndex index = IndexBuilder.create()
                                              .useV10()
                                              .tmpDir(tmpDir)
                                              .schema(schema)
                                              .rows(rows)
                                              .buildMMappedIndex();

    final SegmentId segmentId = SegmentId.dummy(DATASOURCE);
    final QueryRunner<SegmentAnalysis> runner = QueryRunnerTestHelper.makeQueryRunner(
        FACTORY,
        segmentId,
        new QueryableIndexSegment(index, segmentId),
        null
    );

    final SegmentMetadataQuery query =
        Druids.newSegmentMetadataQueryBuilder()
              .dataSource(DATASOURCE)
              .intervals("2013/2014")
              .analysisTypes(SegmentMetadataQuery.AnalysisType.CONTAINERSIZE)
              .merge(false)
              .build();

    final List<SegmentAnalysis> results = runner.run(QueryPlus.wrap(query)).toList();
    Assert.assertEquals(1, results.size());

    final List<ContainerAnalysis> containers = results.get(0).getContainers();
    Assert.assertNotNull(containers);
    Assert.assertEquals(2, containers.size());
    Assert.assertEquals(Projections.BASE_TABLE_PROJECTION_NAME, containers.get(0).bundle());
    Assert.assertEquals(PROJECTION_NAME, containers.get(1).bundle());

    final long baseSize = containers.get(0).size();
    final long projectionSize = containers.get(1).size();
    // loose upper bound: 2 rows of data should never legitimately serialize to this many bytes; this only catches
    // gross errors (e.g. reporting an offset or a whole-file size instead of the container's own size).
    Assert.assertTrue("projection container size should be positive", projectionSize > 0);
    // the base table has an extra __time column that the (time-less) projection doesn't, so it should be larger.
    Assert.assertTrue(
        "base table container should be larger than the projection's (extra __time column)",
        baseSize > projectionSize
    );
    Assert.assertTrue("base table container size looks implausibly large", baseSize < 10_000);
  }
}
