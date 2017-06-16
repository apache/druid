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

package io.druid.segment;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.CharSource;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.select.EventHolder;
import io.druid.query.select.PagingSpec;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectQueryConfig;
import io.druid.query.select.SelectQueryEngine;
import io.druid.query.select.SelectQueryQueryToolChest;
import io.druid.query.select.SelectQueryRunnerFactory;
import io.druid.query.select.SelectResultValue;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.druid.query.QueryRunnerTestHelper.allGran;
import static io.druid.query.QueryRunnerTestHelper.dataSource;
import static io.druid.query.QueryRunnerTestHelper.fullOnInterval;
import static io.druid.query.QueryRunnerTestHelper.makeQueryRunner;
import static io.druid.query.QueryRunnerTestHelper.transformToConstructionFeeder;

/**
 */
@RunWith(Parameterized.class)
public class MapVirtualColumnTest
{
  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    final Supplier<SelectQueryConfig> selectConfigSupplier = Suppliers.ofInstance(new SelectQueryConfig(true));

    SelectQueryRunnerFactory factory = new SelectQueryRunnerFactory(
        new SelectQueryQueryToolChest(
            new DefaultObjectMapper(),
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator(),
            selectConfigSupplier
        ),
        new SelectQueryEngine(selectConfigSupplier),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );

    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(new DateTime("2011-01-12T00:00:00.000Z").getMillis())
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
        )
        , "utf8"
    );

    CharSource input = CharSource.wrap(
        "2011-01-12T00:00:00.000Z\ta\tkey1,key2,key3\tvalue1,value2,value3\n" +
        "2011-01-12T00:00:00.000Z\tb\tkey4,key5,key6\tvalue4\n" +
        "2011-01-12T00:00:00.000Z\tc\tkey1,key5\tvalue1,value5,value9\n"
    );

    IncrementalIndex index1 = TestIndex.loadIncrementalIndex(index, input, parser);
    QueryableIndex index2 = TestIndex.persistRealtimeAndLoadMMapped(index1);

    return transformToConstructionFeeder(
        Arrays.asList(
            makeQueryRunner(factory, "index1", new IncrementalIndexSegment(index1, "index1"), "incremental"),
            makeQueryRunner(factory, "index2", new QueryableIndexSegment("index2", index2), "queryable")
        )
    );
  }

  private final QueryRunner runner;

  public MapVirtualColumnTest(QueryRunner runner)
  {
    this.runner = runner;
  }

  private Druids.SelectQueryBuilder testBuilder()
  {
    return Druids.newSelectQueryBuilder()
                 .dataSource(dataSource)
                 .granularity(allGran)
                 .intervals(fullOnInterval)
                 .pagingSpec(new PagingSpec(null, 3));
  }

  @Test
  public void testBasic() throws Exception
  {
    Druids.SelectQueryBuilder builder = testBuilder();

    List<Map> expectedResults = Arrays.<Map>asList(
        mapOf(
            "dim", "a",
            "params.key1", "value1",
            "params.key3", "value3",
            "params.key5", null,
            "params", mapOf("key1", "value1", "key2", "value2", "key3", "value3")
        ),
        mapOf(
            "dim", "b",
            "params.key1", null,
            "params.key3", null,
            "params.key5", null,
            "params", mapOf("key4", "value4")
        ),
        mapOf(
            "dim", "c",
            "params.key1", "value1",
            "params.key3", null,
            "params.key5", "value5",
            "params", mapOf("key1", "value1", "key5", "value5")
        )
    );
    List<VirtualColumn> virtualColumns = Collections.singletonList(new MapVirtualColumn("keys", "values", "params"));
    SelectQuery selectQuery = builder.dimensions(Collections.singletonList("dim"))
                                     .metrics(Arrays.asList("params.key1", "params.key3", "params.key5", "params"))
                                     .virtualColumns(virtualColumns)
                                     .build();
    checkSelectQuery(selectQuery, expectedResults);
  }

  private Map mapOf(Object... elements)
  {
    Map map = Maps.newHashMap();
    for (int i = 0; i < elements.length; i += 2) {
      map.put(elements[i], elements[i + 1]);
    }
    return map;
  }

  private void checkSelectQuery(SelectQuery searchQuery, List<Map> expected) throws Exception
  {
    List<Result<SelectResultValue>> results = Sequences.toList(
        runner.run(searchQuery, ImmutableMap.of()),
        Lists.<Result<SelectResultValue>>newArrayList()
    );
    Assert.assertEquals(1, results.size());

    List<EventHolder> events = results.get(0).getValue().getEvents();

    Assert.assertEquals(expected.size(), events.size());
    for (int i = 0; i < events.size(); i++) {
      Map event = events.get(i).getEvent();
      event.remove(EventHolder.timestampKey);
      Assert.assertEquals(expected.get(i), event);
    }
  }
}
