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

package org.apache.druid.mapStringString;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.NoopInputRowParser;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexIndexableAdapter;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexAdapter;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapStringStringTypeTest
{
  public static final int NUM_ROWS = 5;

  private static final String TAG0 = "tag0";
  private static final String TAG1 = "tag1";
  private static final String TAG2 = "tag2";
  private static final String TAG3 = "tag3";
  private static final String COUNT = "count";

  public static final String SINGLE_DIM = "tag0-as-single-dim";
  public static final String MULTI_DIM = "tags";

  public static final List<String> DIMENSIONS = ImmutableList.of(
      SINGLE_DIM,
      TAG0,
      TAG1,
      TAG2,
      TAG3
  );

  private static AggregatorFactory[] INDEX_AGGS = new AggregatorFactory[]{
      new CountAggregatorFactory(COUNT)
  };

  private static final DateTime EVENT_TIMESTAMP = DateTimes.of("2020-01-01");
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final AggregationTestHelper groupByQueryTestHelper;
  private final AggregationTestHelper scanQueryTestHelper;

  private final IncrementalIndex incIndex1;
  private final IncrementalIndex incIndex2;
  private final QueryableIndex queryableIndex1;
  private final QueryableIndex queryableIndex2;
  private final QueryableIndex mergedQueryableIndex;

  private final List<Segment> segments;

  public MapStringStringTypeTest() throws Exception
  {
    MapStringStringDruidModule druidModule = new MapStringStringDruidModule();
    druidModule.configure(null);

    groupByQueryTestHelper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        druidModule.getJacksonModules(),
        new GroupByQueryConfig(),
        tempFolder
    );

    scanQueryTestHelper = AggregationTestHelper.createScanQueryAggregationTestHelper(
        druidModule.getJacksonModules(),
        tempFolder
    );

    incIndex1 = makeRealtimeIndex();
    incIndex2 = makeRealtimeIndex();
    queryableIndex1 = TestIndex.persistRealtimeAndLoadMMapped(incIndex1);
    queryableIndex2 = TestIndex.persistRealtimeAndLoadMMapped(incIndex2);

    // Note: using tempFolder.newFolder() at this point errors out with "the temporary folder has not yet been created"
    File someTmpFile = File.createTempFile("MapStringStringType", "test");
    someTmpFile.delete();
    someTmpFile.mkdirs();
    someTmpFile.deleteOnExit();

    File mergedSegmentDir = TestIndex.INDEX_MERGER.merge(
        ImmutableList.of(
            new IncrementalIndexAdapter(
                Intervals.of("1970/2050"),
                incIndex1,
                TestIndex.INDEX_SPEC.getBitmapSerdeFactory().getBitmapFactory()
            ),
            new IncrementalIndexAdapter(
                Intervals.of("1970/2050"),
                incIndex2,
                TestIndex.INDEX_SPEC.getBitmapSerdeFactory().getBitmapFactory()
            ),
            new QueryableIndexIndexableAdapter(queryableIndex1),
            new QueryableIndexIndexableAdapter(queryableIndex2)
        ),
        true,
        INDEX_AGGS,
        someTmpFile,
        TestIndex.INDEX_SPEC
    );

    mergedQueryableIndex = TestIndex.INDEX_IO.loadIndex(mergedSegmentDir);

    segments = ImmutableList.of(
        new IncrementalIndexSegment(incIndex1, SegmentId.dummy("test1")),
        new IncrementalIndexSegment(incIndex1, SegmentId.dummy("test2")),
        new QueryableIndexSegment(queryableIndex1, SegmentId.dummy("test3")),
        new QueryableIndexSegment(queryableIndex2, SegmentId.dummy("test4")),
        new QueryableIndexSegment(mergedQueryableIndex, SegmentId.dummy("test5"))
    );
  }

  private static IncrementalIndex makeRealtimeIndex()
  {
    NullHandling.initializeForTests();
    try {
      List<InputRow> inputRows = Lists.newArrayListWithExpectedSize(NUM_ROWS);
      for (int i = 0; i < NUM_ROWS; i++) {
        String tag0Val = TAG0 + i;
        String tag1Val = TAG1 + i;
        String tag2Val = i % 2 == 0 ? "" : TAG2 + i;
        String tag3Val = i % 2 == 0 ? TAG3 + i : null;

        Map<String, Object> rawData = new HashMap<>();
        rawData.put(SINGLE_DIM, tag0Val);
        rawData.put(TAG0, tag0Val);
        rawData.put(TAG1, tag1Val);
        rawData.put(TAG2, tag2Val);
        rawData.put(TAG3, tag3Val);
        MapBasedInputRow inputRow = new MapBasedInputRow(
            EVENT_TIMESTAMP,
            DIMENSIONS,
            rawData
        );

        // Note: inputRow is added twice to test rollup.
        inputRows.add(inputRow);
        inputRows.add(inputRow);
      }

      return AggregationTestHelper.createIncrementalIndex(
          inputRows.iterator(),
          new MapStringStringSelectorInputRowParser(new NoopInputRowParser(null), MULTI_DIM, Collections.singleton(SINGLE_DIM)),
          Lists.newArrayList(StringDimensionSchema.create(SINGLE_DIM), new MapStringStringDimensionSchema(MULTI_DIM, null)),
          INDEX_AGGS,
          0,
          Granularities.NONE,
          false,
          100,
          true
      );
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Test
  public void testGroupByQuery() throws Exception
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource("test")
        .setGranularity(Granularities.ALL)
        .setInterval("1970/2050")
        .setVirtualColumns(
            new MapStringStringKeyVirtualColumn(MULTI_DIM, TAG0, "tag0clone"),
            new MapStringStringKeyVirtualColumn(MULTI_DIM, TAG1, TAG1),
            new MapStringStringKeyVirtualColumn(MULTI_DIM, TAG2, TAG2),
            new MapStringStringKeyVirtualColumn(MULTI_DIM, TAG3, TAG3),
            new MapStringStringKeyVirtualColumn(MULTI_DIM, "nonExistingKey", "nonExistingDim")
        )
        .addDimension(SINGLE_DIM)
        .addDimension("tag0clone")
        .addDimension(TAG1)
        .addDimension(TAG2)
        .addDimension(TAG3)
        .addDimension("nonExistingDim")
        .setAggregatorSpecs(new LongSumAggregatorFactory(COUNT, COUNT))
        .setDimFilter(
            new NotDimFilter(new SelectorDimFilter(TAG1, TAG1 + (NUM_ROWS - 1), null))
        )
        .addOrderByColumn("tag0clone")
        .build();

    // do json serialization and deserialization of query to ensure there are no serde issues
    ObjectMapper jsonMapper = groupByQueryTestHelper.getObjectMapper();
    query = (GroupByQuery) jsonMapper.readValue(jsonMapper.writeValueAsString(query), Query.class);

    Sequence<ResultRow> seq = groupByQueryTestHelper.runQueryOnSegmentsObjs(segments, query);

    List<ResultRow> rows = seq.toList();
    Assert.assertEquals(NUM_ROWS - 1, rows.size());

    for (int i = 0; i < rows.size(); i++) {
      Map event = rows.get(i).toMap(query);
      Assert.assertEquals(TAG0 + i, event.get(SINGLE_DIM));
      Assert.assertEquals(TAG0 + i, event.get("tag0clone"));
      Assert.assertEquals(TAG1 + i, event.get(TAG1));
      Assert.assertNull(event.get("nonExistingDim"));
      Assert.assertEquals(16L, event.get(COUNT));

      if (i % 2 == 0) {
        Assert.assertNull(event.get(TAG2));
        Assert.assertEquals(TAG3 + i, event.get(TAG3));
      } else {
        Assert.assertNull(event.get(TAG3));
        Assert.assertEquals(TAG2 + i, event.get(TAG2));
      }
    }
  }

  @Test
  public void testScanQuery() throws Exception
  {
    ScanQuery query = Druids.newScanQueryBuilder()
                            .dataSource("test")
                            .columns(Collections.emptyList())
                            .intervals(new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("1970/2050"))))
                            .limit(Integer.MAX_VALUE)
                            .legacy(false)
                            .build();

    // do json serialization and deserialization of query to ensure there are no serde issues
    ObjectMapper jsonMapper = scanQueryTestHelper.getObjectMapper();
    query = (ScanQuery) jsonMapper.readValue(jsonMapper.writeValueAsString(query), Query.class);

    // Test IncrementalIndex segment
    Sequence<ScanResultValue> seq = scanQueryTestHelper.runQueryOnSegmentsObjs(Collections.singletonList(new IncrementalIndexSegment(incIndex1, SegmentId.dummy("test"))), query);

    List<ScanResultValue> rows = seq.toList();
    Assert.assertEquals(1, rows.size());

    ScanResultValue srv = rows.get(0);
    List<Map> events = (List<Map>) srv.getEvents();
    Assert.assertEquals(NUM_ROWS, events.size());
    for (int i = 0; i < events.size(); i++) {
      Map event = events.get(i);
      Assert.assertEquals(EVENT_TIMESTAMP.getMillis(), event.get(ColumnHolder.TIME_COLUMN_NAME));
      Assert.assertEquals(TAG0 + i, event.get(SINGLE_DIM));
      Assert.assertEquals(2, event.get(COUNT));

      if (i % 2 == 0) {
        Assert.assertEquals(ImmutableMap.of(TAG0, TAG0 + i, TAG1, TAG1 + i, TAG3, TAG3 + i), event.get(MULTI_DIM));
      } else {
        Assert.assertEquals(ImmutableMap.of(TAG0, TAG0 + i, TAG1, TAG1 + i, TAG2, TAG2 + i), event.get(MULTI_DIM));
      }
    }

    // Test persisted segment
    seq = scanQueryTestHelper.runQueryOnSegmentsObjs(Collections.singletonList(new QueryableIndexSegment(queryableIndex1, SegmentId.dummy("test"))), query);

    rows = seq.toList();
    Assert.assertEquals(1, rows.size());

    srv = rows.get(0);
    events = (List<Map>) srv.getEvents();
    Assert.assertEquals(NUM_ROWS, events.size());
    for (int i = 0; i < events.size(); i++) {
      Map event = events.get(i);
      Assert.assertEquals(EVENT_TIMESTAMP.getMillis(), event.get(ColumnHolder.TIME_COLUMN_NAME));
      Assert.assertEquals(TAG0 + i, event.get(SINGLE_DIM));
      Assert.assertEquals(2, event.get(COUNT));

      if (i % 2 == 0) {
        Assert.assertEquals(ImmutableMap.of(TAG0, TAG0 + i, TAG1, TAG1 + i, TAG3, TAG3 + i), event.get(MULTI_DIM));
      } else {
        Assert.assertEquals(ImmutableMap.of(TAG0, TAG0 + i, TAG1, TAG1 + i, TAG2, TAG2 + i), event.get(MULTI_DIM));
      }
    }

    // Test merged-and-persisted segment
    seq = scanQueryTestHelper.runQueryOnSegmentsObjs(Collections.singletonList(new QueryableIndexSegment(mergedQueryableIndex, SegmentId.dummy("test"))), query);

    rows = seq.toList();
    Assert.assertEquals(1, rows.size());

    srv = rows.get(0);
    events = (List<Map>) srv.getEvents();
    Assert.assertEquals(NUM_ROWS, events.size());
    for (int i = 0; i < events.size(); i++) {
      Map event = events.get(i);
      Assert.assertEquals(EVENT_TIMESTAMP.getMillis(), event.get(ColumnHolder.TIME_COLUMN_NAME));
      Assert.assertEquals(TAG0 + i, event.get(SINGLE_DIM));
      Assert.assertEquals(8, event.get(COUNT));

      if (i % 2 == 0) {
        Assert.assertEquals(ImmutableMap.of(TAG0, TAG0 + i, TAG1, TAG1 + i, TAG3, TAG3 + i), event.get(MULTI_DIM));
      } else {
        Assert.assertEquals(ImmutableMap.of(TAG0, TAG0 + i, TAG1, TAG1 + i, TAG2, TAG2 + i), event.get(MULTI_DIM));
      }
    }
  }
}
