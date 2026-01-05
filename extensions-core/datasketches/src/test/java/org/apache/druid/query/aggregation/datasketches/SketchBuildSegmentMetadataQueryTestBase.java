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

package org.apache.druid.query.aggregation.datasketches;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.DirectQueryProcessingPool;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.metadata.SegmentMetadataQueryConfig;
import org.apache.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import org.apache.druid.query.metadata.SegmentMetadataQueryRunnerFactory;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Test case for issue https://github.com/apache/druid/issues/14315
 * <p>
 * This test verifies that SegmentMetadataQuery works correctly when querying across
 * IncrementalIndex (realtime) and QueryableIndex (historical) segments that were created with various sketches,
 * ensuring that both segment types report the same column type (COMPLEX<HLLSketch>) by using the serde's normal type.
 */
public abstract class SketchBuildSegmentMetadataQueryTestBase extends InitializedNullHandlingTest
{
  protected static final String DATA_SOURCE = "test_datasource";
  protected static final String SKETCH_COLUMN = "sketch";
  protected static final String DIM_COLUMN = "dim";

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private IndexIO indexIO;
  private IndexMergerV9 indexMerger;
  private QueryRunnerFactory<SegmentAnalysis, SegmentMetadataQuery> queryRunnerFactory;
  private Closer closer;

  @Before
  public void setUp()
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    registerSerdeAndModules(jsonMapper);

    indexIO = new IndexIO(
        jsonMapper,
        new ColumnConfig()
        {
        }
    );
    indexMerger = new IndexMergerV9(
        jsonMapper,
        indexIO,
        OffHeapMemorySegmentWriteOutMediumFactory.instance()
    );
    queryRunnerFactory = new SegmentMetadataQueryRunnerFactory(
        new SegmentMetadataQueryQueryToolChest(new SegmentMetadataQueryConfig()),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    closer = Closer.create();
  }

  protected abstract void registerSerdeAndModules(ObjectMapper jsonMapper);

  /**
   * @return an aggregator that builds a sketch from raw input (its intermediate type may be a non-canonical
   *         "build" type name).
   */
  protected abstract AggregatorFactory buildSketchAggregatorFactory(String sketchColumn, String inputFieldName);

  /** @return the canonical complex type that should be reported by SegmentMetadataQuery. */
  protected abstract ColumnType expectedCanonicalColumnType();

  /** Validate the combining/merged aggregator returned by SegmentMetadataQuery. */
  protected abstract void assertMergedSketchAggregator(AggregatorFactory aggregator, String sketchColumn);

  @Test
  public void testSegmentMetadataQueryWithBuildAggregatorAcrossMixedSegments() throws Exception
  {
    final AggregatorFactory[] aggregators = new AggregatorFactory[]{
        new CountAggregatorFactory("count"),
        buildSketchAggregatorFactory(SKETCH_COLUMN, "id")
    };

    // Create an IncrementalIndex (simulates realtime/unpersisted segment)
    IncrementalIndex incrementalIndex = closer.register(createIncrementalIndex(aggregators));
    addRows(incrementalIndex, 0, 100);

    // Create a QueryableIndex (simulates historical/persisted segment)
    IncrementalIndex indexToPersist = closer.register(createIncrementalIndex(aggregators));
    addRows(indexToPersist, 100, 200);
    File segmentDir = tempFolder.newFolder();
    indexMerger.persist(indexToPersist, segmentDir, IndexSpec.getDefault(), null);
    QueryableIndex queryableIndex = closer.register(indexIO.loadIndex(segmentDir));

    // Create both types of segments
    Segment realtimeSegment = new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy(DATA_SOURCE));
    Segment historicalSegment = new QueryableIndexSegment(queryableIndex, SegmentId.dummy(DATA_SOURCE));
    SegmentMetadataQuery query = Druids.newSegmentMetadataQueryBuilder()
                                       .dataSource(DATA_SOURCE)
                                       .intervals(Collections.singletonList(Intervals.ETERNITY))
                                       .merge(true)
                                       .analysisTypes(SegmentMetadataQuery.AnalysisType.AGGREGATORS)
                                       .build();
    QueryToolChest<SegmentAnalysis, SegmentMetadataQuery> toolChest = queryRunnerFactory.getToolchest();
    QueryRunner<SegmentAnalysis> mergedRunner = toolChest.mergeResults(
        queryRunnerFactory.mergeRunners(
            DirectQueryProcessingPool.INSTANCE,
            List.of(
                toolChest.preMergeQueryDecoration(queryRunnerFactory.createRunner(realtimeSegment)),
                toolChest.preMergeQueryDecoration(queryRunnerFactory.createRunner(historicalSegment))
            )
        )
    );

    Sequence<SegmentAnalysis> results = mergedRunner.run(QueryPlus.wrap(query));
    List<SegmentAnalysis> resultList = results.toList();

    Assert.assertEquals(1, resultList.size());
    SegmentAnalysis mergedAnalysis = resultList.get(0);

    // Verify row count: 100 rows per segment, no rollup due to unique timestamps
    Assert.assertEquals("Merged row count should be 200", 200, mergedAnalysis.getNumRows());

    Assert.assertEquals("Should have 4 columns", 4, mergedAnalysis.getColumns().size());
    Assert.assertTrue("Should contain __time column", mergedAnalysis.getColumns().containsKey("__time"));
    Assert.assertTrue("Should contain dim column", mergedAnalysis.getColumns().containsKey(DIM_COLUMN));
    Assert.assertTrue("Should contain count column", mergedAnalysis.getColumns().containsKey("count"));
    Assert.assertTrue("Should contain sketch column", mergedAnalysis.getColumns().containsKey(SKETCH_COLUMN));

    // Verify no column has merge errors
    for (Map.Entry<String, ColumnAnalysis> entry : mergedAnalysis.getColumns().entrySet()) {
      Assert.assertFalse(
          "Column '" + entry.getKey() + "' should not have error: " + entry.getValue().getErrorMessage(),
          entry.getValue().isError()
      );
    }

    // Make sure the merge went through ok
    Assert.assertEquals(ColumnType.LONG, mergedAnalysis.getColumns().get("__time").getTypeSignature());
    Assert.assertEquals(ColumnType.STRING, mergedAnalysis.getColumns().get(DIM_COLUMN).getTypeSignature());
    Assert.assertEquals(ColumnType.LONG, mergedAnalysis.getColumns().get("count").getTypeSignature());
    Assert.assertEquals(
        expectedCanonicalColumnType(),
        mergedAnalysis.getColumns().get(SKETCH_COLUMN).getTypeSignature()
    );

    ColumnAnalysis sketchColumnAnalysis = mergedAnalysis.getColumns().get(SKETCH_COLUMN);
    Assert.assertFalse("Sketch column should not have multiple values", sketchColumnAnalysis.isHasMultipleValues());

    Assert.assertNotNull("Aggregators should be present", mergedAnalysis.getAggregators());
    Assert.assertEquals("Should have exactly 2 aggregators", 2, mergedAnalysis.getAggregators().size());
    Assert.assertTrue(
        "Should contain count aggregator",
        mergedAnalysis.getAggregators().containsKey("count")
    );
    Assert.assertTrue(
        "Should contain sketch aggregator",
        mergedAnalysis.getAggregators().containsKey(SKETCH_COLUMN)
    );

    // Verify sketch aggregator
    AggregatorFactory sketchAggregator = mergedAnalysis.getAggregators().get(SKETCH_COLUMN);
    Assert.assertNotNull("Sketch aggregator should not be null", sketchAggregator);
    assertMergedSketchAggregator(sketchAggregator, SKETCH_COLUMN);
  }

  /**
   * Verifies that SegmentMetadataQuery works correctly when only querying
   * persisted (historical) segments.
   */
  @Test
  public void testSegmentMetadataQueryWithOnlyPersistedSegments() throws Exception
  {
    final AggregatorFactory[] aggregators = new AggregatorFactory[]{
        new CountAggregatorFactory("count"),
        buildSketchAggregatorFactory(SKETCH_COLUMN, "id")
    };

    // Create two persisted segments
    IncrementalIndex index1 = closer.register(createIncrementalIndex(aggregators));
    addRows(index1, 0, 100);
    File segmentDir1 = tempFolder.newFolder();
    indexMerger.persist(index1, segmentDir1, IndexSpec.getDefault(), null);
    QueryableIndex queryableIndex1 = closer.register(indexIO.loadIndex(segmentDir1));

    IncrementalIndex index2 = closer.register(createIncrementalIndex(aggregators));
    addRows(index2, 100, 200);
    File segmentDir2 = tempFolder.newFolder();
    indexMerger.persist(index2, segmentDir2, IndexSpec.getDefault(), null);
    QueryableIndex queryableIndex2 = closer.register(indexIO.loadIndex(segmentDir2));

    Segment segment1 = new QueryableIndexSegment(queryableIndex1, SegmentId.dummy(DATA_SOURCE));
    Segment segment2 = new QueryableIndexSegment(queryableIndex2, SegmentId.dummy(DATA_SOURCE));

    ColumnAnalysis analysis1 = analyzeColumn(segment1, SKETCH_COLUMN);
    ColumnAnalysis analysis2 = analyzeColumn(segment2, SKETCH_COLUMN);

    Assert.assertEquals(
        "Both persisted segments should report the same type",
        analysis1.getTypeSignature(),
        analysis2.getTypeSignature()
    );

    SegmentMetadataQuery query = Druids.newSegmentMetadataQueryBuilder()
                                       .dataSource(DATA_SOURCE)
                                       .intervals(Collections.singletonList(Intervals.ETERNITY))
                                       .merge(true)
                                       .build();
    QueryToolChest<SegmentAnalysis, SegmentMetadataQuery> toolChest = queryRunnerFactory.getToolchest();
    QueryRunner<SegmentAnalysis> mergedRunner = toolChest.mergeResults(
        queryRunnerFactory.mergeRunners(
            DirectQueryProcessingPool.INSTANCE,
            List.of(
                toolChest.preMergeQueryDecoration(queryRunnerFactory.createRunner(segment1)),
                toolChest.preMergeQueryDecoration(queryRunnerFactory.createRunner(segment2))
            )
        )
    );

    Sequence<SegmentAnalysis> results = mergedRunner.run(QueryPlus.wrap(query));
    List<SegmentAnalysis> resultList = results.toList();

    Assert.assertEquals(1, resultList.size());
    SegmentAnalysis mergedAnalysis = resultList.get(0);

    ColumnAnalysis sketchColumnAnalysis = mergedAnalysis.getColumns().get(SKETCH_COLUMN);
    Assert.assertNotNull("Sketch column should be present", sketchColumnAnalysis);
    Assert.assertFalse(
        "No error expected when both segments are persisted: " + sketchColumnAnalysis.getErrorMessage(),
        sketchColumnAnalysis.isError()
    );

    Assert.assertEquals(
        expectedCanonicalColumnType(),
        sketchColumnAnalysis.getTypeSignature()
    );
  }

  /**
   * Verifies that SegmentMetadataQuery works correctly when only querying
   * realtime (IncrementalIndex) segments.
   */
  @Test
  public void testSegmentMetadataQueryWithOnlyRealtimeSegments()
  {
    final AggregatorFactory[] aggregators = new AggregatorFactory[]{
        new CountAggregatorFactory("count"),
        buildSketchAggregatorFactory(SKETCH_COLUMN, "id")
    };

    // Create two realtime (IncrementalIndex) segments
    IncrementalIndex index1 = closer.register(createIncrementalIndex(aggregators));
    addRows(index1, 0, 100);

    IncrementalIndex index2 = closer.register(createIncrementalIndex(aggregators));
    addRows(index2, 100, 200);

    Segment segment1 = new IncrementalIndexSegment(index1, SegmentId.dummy(DATA_SOURCE));
    Segment segment2 = new IncrementalIndexSegment(index2, SegmentId.dummy(DATA_SOURCE));

    ColumnAnalysis analysis1 = analyzeColumn(segment1, SKETCH_COLUMN);
    ColumnAnalysis analysis2 = analyzeColumn(segment2, SKETCH_COLUMN);

    Assert.assertEquals(
        "Both realtime segments should report the same type",
        analysis1.getTypeSignature(),
        analysis2.getTypeSignature()
    );

    SegmentMetadataQuery query = Druids.newSegmentMetadataQueryBuilder()
                                       .dataSource(DATA_SOURCE)
                                       .intervals(Collections.singletonList(Intervals.ETERNITY))
                                       .merge(true)
                                       .build();
    QueryToolChest<SegmentAnalysis, SegmentMetadataQuery> toolChest = queryRunnerFactory.getToolchest();
    QueryRunner<SegmentAnalysis> mergedRunner = toolChest.mergeResults(
        queryRunnerFactory.mergeRunners(
            DirectQueryProcessingPool.INSTANCE,
            List.of(
                toolChest.preMergeQueryDecoration(queryRunnerFactory.createRunner(segment1)),
                toolChest.preMergeQueryDecoration(queryRunnerFactory.createRunner(segment2))
            )
        )
    );

    Sequence<SegmentAnalysis> results = mergedRunner.run(QueryPlus.wrap(query));
    List<SegmentAnalysis> resultList = results.toList();

    Assert.assertEquals(1, resultList.size());
    SegmentAnalysis mergedAnalysis = resultList.get(0);

    ColumnAnalysis sketchColumnAnalysis = mergedAnalysis.getColumns().get(SKETCH_COLUMN);
    Assert.assertNotNull("Sketch column should be present", sketchColumnAnalysis);
    Assert.assertFalse(
        "No error expected when both segments are realtime: " + sketchColumnAnalysis.getErrorMessage(),
        sketchColumnAnalysis.isError()
    );

    Assert.assertEquals(
        expectedCanonicalColumnType(),
        sketchColumnAnalysis.getTypeSignature()
    );
  }

  private IncrementalIndex createIncrementalIndex(AggregatorFactory[] aggregators)
  {
    return new OnheapIncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withMinTimestamp(DateTimes.of("2020-01-01").getMillis())
                .withQueryGranularity(Granularities.NONE)
                .withMetrics(aggregators)
                .withRollup(true)
                .build()
        )
        .setMaxRowCount(1000)
        .build();
  }

  private void addRows(IncrementalIndex index, int startId, int endId)
  {
    for (int i = startId; i < endId; i++) {
      Map<String, Object> event = Map.of(
          "id", "user_" + i,
          DIM_COLUMN, "dim_value_" + (i % 10)
      );
      index.add(new MapBasedInputRow(
          DateTimes.of("2020-01-01").getMillis() + i,
          Collections.singletonList(DIM_COLUMN),
          event
      ));
    }
  }

  private ColumnAnalysis analyzeColumn(Segment segment, String columnName)
  {
    SegmentMetadataQuery query = Druids.newSegmentMetadataQueryBuilder()
                                       .dataSource(DATA_SOURCE)
                                       .intervals(Collections.singletonList(Intervals.ETERNITY))
                                       .build();

    QueryRunner<SegmentAnalysis> runner = queryRunnerFactory.createRunner(segment);
    List<SegmentAnalysis> results = runner.run(QueryPlus.wrap(query)).toList();

    Assert.assertEquals(1, results.size());
    return results.get(0).getColumns().get(columnName);
  }
}


