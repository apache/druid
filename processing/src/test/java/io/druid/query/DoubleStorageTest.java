/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.query;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.metadata.SegmentMetadataQueryConfig;
import io.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import io.druid.query.metadata.SegmentMetadataQueryRunnerFactory;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.ListColumnIncluderator;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;

import io.druid.query.scan.ScanQuery;
import io.druid.query.scan.ScanQueryConfig;
import io.druid.query.scan.ScanQueryEngine;
import io.druid.query.scan.ScanQueryQueryToolChest;
import io.druid.query.scan.ScanQueryRunnerFactory;
import io.druid.query.scan.ScanResultValue;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.TestHelper;
import io.druid.segment.column.ValueType;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.IndexSizeExceededException;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.druid.segment.column.Column.DOUBLE_STORAGE_TYPE_PROPERTY;
import static io.druid.query.scan.ScanQueryRunnerTest.verify;

@RunWith(Parameterized.class)
public class DoubleStorageTest
{

  private static final SegmentMetadataQueryRunnerFactory METADATA_QR_FACTORY = new SegmentMetadataQueryRunnerFactory(
      new SegmentMetadataQueryQueryToolChest(new SegmentMetadataQueryConfig()),
      QueryRunnerTestHelper.NOOP_QUERYWATCHER
  );

  private static final ScanQueryQueryToolChest scanQueryQueryToolChest = new ScanQueryQueryToolChest(
      new ScanQueryConfig(),
      DefaultGenericQueryMetricsFactory.instance()
  );

  private static final ScanQueryRunnerFactory SCAN_QUERY_RUNNER_FACTORY = new ScanQueryRunnerFactory(
      scanQueryQueryToolChest,
      new ScanQueryEngine()
  );

  private ScanQuery.ScanQueryBuilder newTestQuery()
  {
    return ScanQuery.newScanQueryBuilder()
                    .dataSource(new TableDataSource(QueryRunnerTestHelper.dataSource))
                    .columns(Arrays.<String>asList())
                    .intervals(QueryRunnerTestHelper.fullOnInterval)
                    .limit(Integer.MAX_VALUE)
                    .legacy(false);
  }


  private static final IndexMergerV9 INDEX_MERGER_V9 = TestHelper.getTestIndexMergerV9();
  private static final IndexIO INDEX_IO = TestHelper.getTestIndexIO();
  private static final Integer MAX_ROWS = 10;
  private static final String TIME_COLUMN = "__time";
  private static final String DIM_NAME = "testDimName";
  private static final String DIM_VALUE = "testDimValue";
  private static final String DIM_FLOAT_NAME = "testDimFloatName";
  private static final String SEGMENT_ID = "segmentId";
  private static final Interval INTERVAL = Intervals.of("2011-01-13T00:00:00.000Z/2011-01-22T00:00:00.001Z");

  private static final InputRowParser<Map<String, Object>> ROW_PARSER = new MapInputRowParser(
      new JSONParseSpec(
          new TimestampSpec(TIME_COLUMN, "auto", null),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of(DIM_NAME)),
              ImmutableList.of(DIM_FLOAT_NAME),
              ImmutableList.<SpatialDimensionSchema>of()
          ),
          null,
          null
      )
  );

  private QueryableIndex index;
  private final SegmentAnalysis expectedSegmentAnalysis;
  private final String storeDoubleAs;

  public DoubleStorageTest(
      String storeDoubleAs,
      SegmentAnalysis expectedSegmentAnalysis
  ) throws IOException
  {
    this.storeDoubleAs = storeDoubleAs;
    this.expectedSegmentAnalysis = expectedSegmentAnalysis;
  }

  @Parameterized.Parameters
  public static Collection<?> dataFeeder() throws IOException
  {
    SegmentAnalysis expectedSegmentAnalysisDouble = new SegmentAnalysis(
        "segmentId",
        ImmutableList.of(INTERVAL),
        ImmutableMap.of(
            TIME_COLUMN,
            new ColumnAnalysis(
                ValueType.LONG.toString(),
                false,
                100,
                null,
                null,
                null,
                null
            ),
            DIM_NAME,
            new ColumnAnalysis(
                ValueType.STRING.toString(),
                false,
                120,
                1,
                DIM_VALUE,
                DIM_VALUE,
                null
            ),
            DIM_FLOAT_NAME,
            new ColumnAnalysis(
                ValueType.DOUBLE.toString(),
                false,
                80,
                null,
                null,
                null,
                null
            )
        ), 330,
        MAX_ROWS,
        null,
        null,
        null,
        null
    );

    SegmentAnalysis expectedSegmentAnalysisFloat = new SegmentAnalysis(
        "segmentId",
        ImmutableList.of(INTERVAL),
        ImmutableMap.of(
            TIME_COLUMN,
            new ColumnAnalysis(
                ValueType.LONG.toString(),
                false,
                100,
                null,
                null,
                null,
                null
            ),
            DIM_NAME,
            new ColumnAnalysis(
                ValueType.STRING.toString(),
                false,
                120,
                1,
                DIM_VALUE,
                DIM_VALUE,
                null
            ),
            DIM_FLOAT_NAME,
            new ColumnAnalysis(
                ValueType.FLOAT.toString(),
                false,
                80,
                null,
                null,
                null,
                null
            )
        ), 330,
        MAX_ROWS,
        null,
        null,
        null,
        null
    );

    return ImmutableList.of(
        new Object[]{"double", expectedSegmentAnalysisDouble},
        new Object[]{"float", expectedSegmentAnalysisFloat}
    );
  }

  @Before
  public void setup() throws IOException
  {
    index = buildIndex(storeDoubleAs);
  }

  @Test
  public void testMetaDataAnalysis() throws IndexSizeExceededException
  {
    QueryRunner runner = QueryRunnerTestHelper.makeQueryRunner(
        METADATA_QR_FACTORY,
        SEGMENT_ID,
        new QueryableIndexSegment("segmentId", index),
        null
    );


    SegmentMetadataQuery segmentMetadataQuery = Druids.newSegmentMetadataQueryBuilder()
                                                      .dataSource("testing")
                                                      .intervals(ImmutableList.of(INTERVAL))
                                                      .toInclude(new ListColumnIncluderator(Arrays.asList(
                                                          TIME_COLUMN,
                                                          DIM_NAME,
                                                          DIM_FLOAT_NAME
                                                      )))
                                                      .analysisTypes(
                                                          SegmentMetadataQuery.AnalysisType.CARDINALITY,
                                                          SegmentMetadataQuery.AnalysisType.SIZE,
                                                          SegmentMetadataQuery.AnalysisType.INTERVAL,
                                                          SegmentMetadataQuery.AnalysisType.MINMAX
                                                      )
                                                      .merge(true)
                                                      .build();
    List<SegmentAnalysis> results = Sequences.toList(
        runner.run(QueryPlus.wrap(segmentMetadataQuery), Maps.newHashMap()),
        Lists.<SegmentAnalysis>newArrayList()
    );

    Assert.assertEquals(Arrays.asList(expectedSegmentAnalysis), results);

  }

  @Test
  public void testSelectValues()
  {
    QueryRunner runner = QueryRunnerTestHelper.makeQueryRunner(
        SCAN_QUERY_RUNNER_FACTORY,
        SEGMENT_ID,
        new QueryableIndexSegment("segmentId", index),
        null
    );

    ScanQuery query = newTestQuery()
        .intervals(new LegacySegmentSpec(INTERVAL))
        .virtualColumns()
        .build();

    HashMap<String, Object> context = new HashMap<String, Object>();
    Iterable<ScanResultValue> results = Sequences.toList(
        runner.run(QueryPlus.wrap(query), context),
        Lists.<ScanResultValue>newArrayList()
    );

    ScanResultValue expectedScanResult = new ScanResultValue(
        SEGMENT_ID,
        ImmutableList.of(TIME_COLUMN, DIM_NAME, DIM_FLOAT_NAME),
        getStreamOfEvents().collect(Collectors.toList())
    );
    List<ScanResultValue> expectedResults = Lists.newArrayList(expectedScanResult);
    verify(expectedResults, results);
  }

  private static QueryableIndex buildIndex(String storeDoubleAsFloat) throws IOException
  {
    String oldValue = System.getProperty(DOUBLE_STORAGE_TYPE_PROPERTY);
    System.setProperty(DOUBLE_STORAGE_TYPE_PROPERTY, storeDoubleAsFloat);
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(DateTimes.of("2011-01-13T00:00:00.000Z").getMillis())
        .withDimensionsSpec(ROW_PARSER)
        .withMetrics(
            new DoubleSumAggregatorFactory(DIM_FLOAT_NAME, DIM_FLOAT_NAME)
        )
        .build();

    final IncrementalIndex index = new IncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(MAX_ROWS)
        .buildOnheap();


    getStreamOfEvents().forEach(o -> {
      try {
        index.add(ROW_PARSER.parse((Map) o));
      }
      catch (IndexSizeExceededException e) {
        Throwables.propagate(e);
      }
    });

    if (oldValue == null) {
      System.clearProperty(DOUBLE_STORAGE_TYPE_PROPERTY);
    } else {
      System.setProperty(DOUBLE_STORAGE_TYPE_PROPERTY, oldValue);
    }
    File someTmpFile = File.createTempFile("billy", "yay");
    someTmpFile.delete();
    someTmpFile.mkdirs();
    INDEX_MERGER_V9.persist(index, someTmpFile, new IndexSpec());
    someTmpFile.delete();
    return INDEX_IO.loadIndex(someTmpFile);
  }

  @After
  public void cleanUp() throws IOException
  {
    index.close();
  }

  private static Stream getStreamOfEvents()
  {
    return IntStream.range(0, MAX_ROWS).mapToObj(i -> ImmutableMap.of(
        TIME_COLUMN, DateTimes.of("2011-01-13T00:00:00.000Z").plusDays(i).getMillis(),
        DIM_NAME, DIM_VALUE,
        DIM_FLOAT_NAME, i / 1.6179775280898876
    ));
  }

}
