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

package org.apache.druid.segment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.io.CharSource;
import org.apache.druid.data.input.impl.DelimitedParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.select.EventHolder;
import org.apache.druid.query.select.PagingSpec;
import org.apache.druid.query.select.SelectQuery;
import org.apache.druid.query.select.SelectQueryConfig;
import org.apache.druid.query.select.SelectQueryEngine;
import org.apache.druid.query.select.SelectQueryQueryToolChest;
import org.apache.druid.query.select.SelectQueryRunnerFactory;
import org.apache.druid.query.select.SelectResultValue;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class MapVirtualColumnSelectTest
{
  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    final Supplier<SelectQueryConfig> selectConfigSupplier = Suppliers.ofInstance(new SelectQueryConfig(true));

    SelectQueryRunnerFactory factory = new SelectQueryRunnerFactory(
        new SelectQueryQueryToolChest(
            new DefaultObjectMapper(),
            QueryRunnerTestHelper.noopIntervalChunkingQueryRunnerDecorator()
        ),
        new SelectQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );

    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(DateTimes.of("2011-01-12T00:00:00.000Z").getMillis())
        .build();
    final IncrementalIndex index = new IncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(10000)
        .buildOnheap();

    final StringInputRowParser parser = new StringInputRowParser(
        new DelimitedParseSpec(
            new TimestampSpec("ts", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("dim", "keys", "values")), null, null),
            "\t",
            ",",
            Arrays.asList("ts", "dim", "keys", "values"),
            false,
            0
        ),
        "utf8"
    );

    CharSource input = CharSource.wrap(
        "2011-01-12T00:00:00.000Z\ta\tkey1,key2,key3\tvalue1,value2,value3\n" +
        "2011-01-12T00:00:00.000Z\tb\tkey4,key5,key6\tvalue4\n" +
        "2011-01-12T00:00:00.000Z\tc\tkey1,key5\tvalue1,value5,value9\n"
    );

    IncrementalIndex index1 = TestIndex.loadIncrementalIndex(() -> index, input, parser);
    QueryableIndex index2 = TestIndex.persistRealtimeAndLoadMMapped(index1);

    return QueryRunnerTestHelper.transformToConstructionFeeder(
        Arrays.asList(
            QueryRunnerTestHelper.makeQueryRunner(
                factory,
                SegmentId.dummy("index1"),
                new IncrementalIndexSegment(index1, SegmentId.dummy("index1")),
                "incremental"
            ),
            QueryRunnerTestHelper.makeQueryRunner(
                factory,
                SegmentId.dummy("index2"),
                new QueryableIndexSegment(index2, SegmentId.dummy("index2")),
                "queryable"
            )
        )
    );
  }

  private final QueryRunner runner;

  public MapVirtualColumnSelectTest(QueryRunner runner)
  {
    this.runner = runner;
  }

  private Druids.SelectQueryBuilder testBuilder()
  {
    return Druids.newSelectQueryBuilder()
                 .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                 .granularity(QueryRunnerTestHelper.ALL_GRAN)
                 .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                 .pagingSpec(new PagingSpec(null, 3));
  }

  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    new DruidVirtualColumnsModule().getJacksonModules().forEach(mapper::registerModule);

    final MapVirtualColumn column = new MapVirtualColumn("keys", "values", "params");
    final String json = mapper.writeValueAsString(column);
    final VirtualColumn fromJson = mapper.readValue(json, VirtualColumn.class);
    Assert.assertEquals(column, fromJson);
  }

  @Test
  public void testBasic()
  {
    Druids.SelectQueryBuilder builder = testBuilder();

    List<Map> expectedResults = Arrays.asList(
        MapVirtualColumnTestBase.mapOf(
            "dim", "a",
            "params.key1", "value1",
            "params.key3", "value3",
            "params.key5", null,
            "params", MapVirtualColumnTestBase.mapOf("key1", "value1", "key2", "value2", "key3", "value3")
        ),
        MapVirtualColumnTestBase.mapOf(
            "dim", "b",
            "params.key1", null,
            "params.key3", null,
            "params.key5", null,
            "params", MapVirtualColumnTestBase.mapOf("key4", "value4")
        ),
        MapVirtualColumnTestBase.mapOf(
            "dim", "c",
            "params.key1", "value1",
            "params.key3", null,
            "params.key5", "value5",
            "params", MapVirtualColumnTestBase.mapOf("key1", "value1", "key5", "value5")
        )
    );
    List<VirtualColumn> virtualColumns = Collections.singletonList(new MapVirtualColumn("keys", "values", "params"));
    SelectQuery selectQuery = builder.dimensions(Collections.singletonList("dim"))
                                     .metrics(Arrays.asList("params.key1", "params.key3", "params.key5", "params"))
                                     .virtualColumns(virtualColumns)
                                     .build();
    checkSelectQuery(selectQuery, expectedResults);
  }

  private void checkSelectQuery(SelectQuery searchQuery, List<Map> expected)
  {
    List<Result<SelectResultValue>> results = runner.run(QueryPlus.wrap(searchQuery)).toList();
    Assert.assertEquals(1, results.size());

    List<EventHolder> events = results.get(0).getValue().getEvents();

    Assert.assertEquals(expected.size(), events.size());
    for (int i = 0; i < events.size(); i++) {
      Map event = events.get(i).getEvent();
      event.remove(EventHolder.TIMESTAMP_KEY);
      Assert.assertEquals(expected.get(i), event);
    }
  }
}
