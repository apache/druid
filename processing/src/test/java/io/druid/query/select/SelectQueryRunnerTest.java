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

package io.druid.query.select;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.ObjectArrays;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequences;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class SelectQueryRunnerTest
{
  // copied from druid.sample.tsv
  public static final String[] V_0112 = {
      "2011-01-12T00:00:00.000Z	spot	automotive	preferred	apreferred	100.000000",
      "2011-01-12T00:00:00.000Z	spot	business	preferred	bpreferred	100.000000",
      "2011-01-12T00:00:00.000Z	spot	entertainment	preferred	epreferred	100.000000",
      "2011-01-12T00:00:00.000Z	spot	health	preferred	hpreferred	100.000000",
      "2011-01-12T00:00:00.000Z	spot	mezzanine	preferred	mpreferred	100.000000",
      "2011-01-12T00:00:00.000Z	spot	news	preferred	npreferred	100.000000",
      "2011-01-12T00:00:00.000Z	spot	premium	preferred	ppreferred	100.000000",
      "2011-01-12T00:00:00.000Z	spot	technology	preferred	tpreferred	100.000000",
      "2011-01-12T00:00:00.000Z	spot	travel	preferred	tpreferred	100.000000",
      "2011-01-12T00:00:00.000Z	total_market	mezzanine	preferred	mpreferred	1000.000000",
      "2011-01-12T00:00:00.000Z	total_market	premium	preferred	ppreferred	1000.000000",
      "2011-01-12T00:00:00.000Z	upfront	mezzanine	preferred	mpreferred	800.000000	value",
      "2011-01-12T00:00:00.000Z	upfront	premium	preferred	ppreferred	800.000000	value"
  };
  public static final String[] V_0113 = {
      "2011-01-13T00:00:00.000Z	spot	automotive	preferred	apreferred	94.874713",
      "2011-01-13T00:00:00.000Z	spot	business	preferred	bpreferred	103.629399",
      "2011-01-13T00:00:00.000Z	spot	entertainment	preferred	epreferred	110.087299",
      "2011-01-13T00:00:00.000Z	spot	health	preferred	hpreferred	114.947403",
      "2011-01-13T00:00:00.000Z	spot	mezzanine	preferred	mpreferred	104.465767",
      "2011-01-13T00:00:00.000Z	spot	news	preferred	npreferred	102.851683",
      "2011-01-13T00:00:00.000Z	spot	premium	preferred	ppreferred	108.863011",
      "2011-01-13T00:00:00.000Z	spot	technology	preferred	tpreferred	111.356672",
      "2011-01-13T00:00:00.000Z	spot	travel	preferred	tpreferred	106.236928",
      "2011-01-13T00:00:00.000Z	total_market	mezzanine	preferred	mpreferred	1040.945505",
      "2011-01-13T00:00:00.000Z	total_market	premium	preferred	ppreferred	1689.012875",
      "2011-01-13T00:00:00.000Z	upfront	mezzanine	preferred	mpreferred	826.060182	value",
      "2011-01-13T00:00:00.000Z	upfront	premium	preferred	ppreferred	1564.617729	value"
  };

  public static final QuerySegmentSpec I_0112_0114 = new LegacySegmentSpec(
      new Interval("2011-01-12/2011-01-14")
  );
  public static final String[] V_0112_0114 = ObjectArrays.concat(V_0112, V_0113, String.class);

  @Parameterized.Parameters(name = "{0}:descending={1}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.cartesian(
        QueryRunnerTestHelper.makeQueryRunners(
            new SelectQueryRunnerFactory(
                new SelectQueryQueryToolChest(
                    new DefaultObjectMapper(),
                    QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                ),
                new SelectQueryEngine(),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        ),
        Arrays.asList(false, true)
    );
  }

  private final QueryRunner runner;
  private final boolean descending;

  public SelectQueryRunnerTest(QueryRunner runner, boolean descending)
  {
    this.runner = runner;
    this.descending = descending;
  }

  @Test
  public void testFullOnSelect()
  {
    SelectQuery query = new SelectQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        I_0112_0114,
        descending,
        null,
        QueryRunnerTestHelper.allGran,
        Arrays.<String>asList(),
        Arrays.<String>asList(),
        new PagingSpec(null, 3),
        null
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    Iterable<Result<SelectResultValue>> results = Sequences.toList(
        runner.run(query, context),
        Lists.<Result<SelectResultValue>>newArrayList()
    );

    PagingOffset offset = query.getPagingOffset(QueryRunnerTestHelper.segmentId);
    List<Result<SelectResultValue>> expectedResults = toExpected(
        toEvents(new String[]{EventHolder.timestampKey + ":TIME"}, V_0112_0114),
        offset.startOffset(),
        offset.threshold()
    );
    verify(expectedResults, results);
  }

  @Test
  public void testSelectWithDimsAndMets()
  {
    SelectQuery query = new SelectQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        I_0112_0114,
        descending,
        null,
        QueryRunnerTestHelper.allGran,
        Arrays.asList(QueryRunnerTestHelper.marketDimension),
        Arrays.asList(QueryRunnerTestHelper.indexMetric),
        new PagingSpec(null, 3),
        null
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    Iterable<Result<SelectResultValue>> results = Sequences.toList(
        runner.run(query, context),
        Lists.<Result<SelectResultValue>>newArrayList()
    );

    PagingOffset offset = query.getPagingOffset(QueryRunnerTestHelper.segmentId);
    List<Result<SelectResultValue>> expectedResults = toExpected(
        toEvents(
            new String[]{
                EventHolder.timestampKey + ":TIME",
                QueryRunnerTestHelper.marketDimension + ":STRING",
                null,
                null,
                null,
                QueryRunnerTestHelper.indexMetric + ":FLOAT"
            },
            V_0112_0114
        ),
        offset.startOffset(),
        offset.threshold()
    );
    verify(expectedResults, results);
  }

  @Test
  public void testSelectPagination()
  {
    SelectQuery query = new SelectQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        I_0112_0114,
        descending,
        null,
        QueryRunnerTestHelper.allGran,
        Arrays.asList(QueryRunnerTestHelper.qualityDimension),
        Arrays.asList(QueryRunnerTestHelper.indexMetric),
        new PagingSpec(toPagingIdentifier(3, descending), 3),
        null
    );

    Iterable<Result<SelectResultValue>> results = Sequences.toList(
        runner.run(query, Maps.newHashMap()),
        Lists.<Result<SelectResultValue>>newArrayList()
    );

    PagingOffset offset = query.getPagingOffset(QueryRunnerTestHelper.segmentId);
    List<Result<SelectResultValue>> expectedResults = toExpected(
        toEvents(
            new String[]{
                EventHolder.timestampKey + ":TIME",
                "foo:NULL",
                "foo2:NULL"
            },
            V_0112_0114
        ),
        offset.startOffset(),
        offset.threshold()
    );
    verify(expectedResults, results);
  }

  @Test
  public void testFullOnSelectWithFilter()
  {
    // startDelta + threshold pairs
    for (int[] param : new int[][]{{3, 3}, {0, 1}, {5, 5}, {2, 7}, {3, 0}}) {
      SelectQuery query = new SelectQuery(
          new TableDataSource(QueryRunnerTestHelper.dataSource),
          I_0112_0114,
          descending,
          new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "spot"),
          QueryRunnerTestHelper.dayGran,
          Lists.<String>newArrayList(QueryRunnerTestHelper.qualityDimension),
          Lists.<String>newArrayList(QueryRunnerTestHelper.indexMetric),
          new PagingSpec(toPagingIdentifier(param[0], descending), param[1]),
          null
      );
      HashMap<String, Object> context = new HashMap<String, Object>();
      Iterable<Result<SelectResultValue>> results = Sequences.toList(
          runner.run(query, context),
          Lists.<Result<SelectResultValue>>newArrayList()
      );

      final List<List<Map<String, Object>>> events = toEvents(
          new String[]{
              EventHolder.timestampKey + ":TIME",
              null,
              QueryRunnerTestHelper.qualityDimension + ":STRING",
              null,
              null,
              QueryRunnerTestHelper.indexMetric + ":FLOAT"
          },
          // filtered values with day granularity
          new String[]{
              "2011-01-12T00:00:00.000Z	spot	automotive	preferred	apreferred	100.000000",
              "2011-01-12T00:00:00.000Z	spot	business	preferred	bpreferred	100.000000",
              "2011-01-12T00:00:00.000Z	spot	entertainment	preferred	epreferred	100.000000",
              "2011-01-12T00:00:00.000Z	spot	health	preferred	hpreferred	100.000000",
              "2011-01-12T00:00:00.000Z	spot	mezzanine	preferred	mpreferred	100.000000",
              "2011-01-12T00:00:00.000Z	spot	news	preferred	npreferred	100.000000",
              "2011-01-12T00:00:00.000Z	spot	premium	preferred	ppreferred	100.000000",
              "2011-01-12T00:00:00.000Z	spot	technology	preferred	tpreferred	100.000000",
              "2011-01-12T00:00:00.000Z	spot	travel	preferred	tpreferred	100.000000"
          },
          new String[]{
              "2011-01-13T00:00:00.000Z	spot	automotive	preferred	apreferred	94.874713",
              "2011-01-13T00:00:00.000Z	spot	business	preferred	bpreferred	103.629399",
              "2011-01-13T00:00:00.000Z	spot	entertainment	preferred	epreferred	110.087299",
              "2011-01-13T00:00:00.000Z	spot	health	preferred	hpreferred	114.947403",
              "2011-01-13T00:00:00.000Z	spot	mezzanine	preferred	mpreferred	104.465767",
              "2011-01-13T00:00:00.000Z	spot	news	preferred	npreferred	102.851683",
              "2011-01-13T00:00:00.000Z	spot	premium	preferred	ppreferred	108.863011",
              "2011-01-13T00:00:00.000Z	spot	technology	preferred	tpreferred	111.356672",
              "2011-01-13T00:00:00.000Z	spot	travel	preferred	tpreferred	106.236928"
          }
      );

      PagingOffset offset = query.getPagingOffset(QueryRunnerTestHelper.segmentId);
      List<Result<SelectResultValue>> expectedResults = toExpected(
          events,
          offset.startOffset(),
          offset.threshold()
      );
      verify(expectedResults, results);
    }
  }

  @Test
  public void testFullSelectNoResults()
  {
    SelectQuery query = new SelectQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        I_0112_0114,
        descending,
        new AndDimFilter(
            Arrays.<DimFilter>asList(
                new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "spot"),
                new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "foo")
            )
        ),
        QueryRunnerTestHelper.allGran,
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        new PagingSpec(null, 3),
        null
    );

    Iterable<Result<SelectResultValue>> results = Sequences.toList(
        runner.run(query, Maps.newHashMap()),
        Lists.<Result<SelectResultValue>>newArrayList()
    );

    List<Result<SelectResultValue>> expectedResults = Arrays.asList(
        new Result<SelectResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SelectResultValue(
                ImmutableMap.<String, Integer>of(),
                Lists.<EventHolder>newArrayList()
            )
        )
    );

    verify(expectedResults, results);
  }

  @Test
  public void testFullSelectNoDimensionAndMetric()
  {
    SelectQuery query = new SelectQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        I_0112_0114,
        descending,
        null,
        QueryRunnerTestHelper.allGran,
        Lists.<String>newArrayList("foo"),
        Lists.<String>newArrayList("foo2"),
        new PagingSpec(null, 3),
        null
    );

    Iterable<Result<SelectResultValue>> results = Sequences.toList(
        runner.run(query, Maps.newHashMap()),
        Lists.<Result<SelectResultValue>>newArrayList()
    );

    final List<List<Map<String, Object>>> events = toEvents(
        new String[]{
            EventHolder.timestampKey + ":TIME",
            "foo:NULL",
            "foo2:NULL"
        },
        V_0112_0114
    );

    PagingOffset offset = query.getPagingOffset(QueryRunnerTestHelper.segmentId);
    List<Result<SelectResultValue>> expectedResults = toExpected(
        events,
        offset.startOffset(),
        offset.threshold()
    );
    verify(expectedResults, results);
  }

  private LinkedHashMap<String, Integer> toPagingIdentifier(int startDelta, boolean descending)
  {
    return Maps.newLinkedHashMap(
        ImmutableMap.of(
            QueryRunnerTestHelper.segmentId,
            PagingOffset.toOffset(startDelta, descending)
        )
    );
  }

  private List<List<Map<String, Object>>> toEvents(final String[] dimSpecs, final String[]... valueSet)
  {
    List<List<Map<String, Object>>> events = Lists.newArrayList();
    for (String[] values : valueSet) {
      events.add(
          Lists.newArrayList(
              Iterables.transform(
                  Arrays.asList(values), new Function<String, Map<String, Object>>()
                  {
                    @Override
                    public Map<String, Object> apply(String input)
                    {
                      Map<String, Object> event = Maps.newHashMap();
                      String[] values = input.split("\\t");
                      for (int i = 0; i < dimSpecs.length; i++) {
                        if (dimSpecs[i] == null || i >= dimSpecs.length) {
                          continue;
                        }
                        String[] specs = dimSpecs[i].split(":");
                        event.put(
                            specs[0],
                            specs[1].equals("TIME") ? new DateTime(values[i]) :
                            specs[1].equals("FLOAT") ? Float.valueOf(values[i]) :
                            specs[1].equals("DOUBLE") ? Double.valueOf(values[i]) :
                            specs[1].equals("LONG") ? Long.valueOf(values[i]) :
                            specs[1].equals("NULL") ? null :
                            values[i]
                        );
                      }
                      return event;
                    }
                  }
              )
          )
      );
    }
    return events;
  }

  private List<Result<SelectResultValue>> toExpected(
      List<List<Map<String, Object>>> targets,
      final int offset,
      final int threshold
  )
  {
    if (offset < 0) {
      targets = Lists.reverse(targets);
    }
    List<Result<SelectResultValue>> expected = Lists.newArrayListWithExpectedSize(targets.size());
    for (List<Map<String, Object>> group : targets) {
      List<EventHolder> holders = Lists.newArrayListWithExpectedSize(threshold);
      int newOffset = offset;
      if (offset < 0) {
        int start = group.size() + offset;
        int end = Math.max(-1, start - threshold);
        for (int i = start; i > end; i--) {
          holders.add(new EventHolder(QueryRunnerTestHelper.segmentId, newOffset--, group.get(i)));
        }
      } else {
        int end = Math.min(group.size(), offset + threshold);
        for (int i = offset; i < end; i++) {
          holders.add(new EventHolder(QueryRunnerTestHelper.segmentId, newOffset++, group.get(i)));
        }
      }
      int lastOffset = holders.isEmpty() ? offset : holders.get(holders.size() - 1).getOffset();
      expected.add(
          new Result(
              new DateTime(group.get(0).get(EventHolder.timestampKey)),
              new SelectResultValue(ImmutableMap.of(QueryRunnerTestHelper.segmentId, lastOffset), holders)
          )
      );
    }
    return expected;
  }

  private static void verify(
      Iterable<Result<SelectResultValue>> expectedResults,
      Iterable<Result<SelectResultValue>> actualResults
  )
  {
    Iterator<Result<SelectResultValue>> expectedIter = expectedResults.iterator();
    Iterator<Result<SelectResultValue>> actualIter = actualResults.iterator();

    while (expectedIter.hasNext()) {
      Result<SelectResultValue> expected = expectedIter.next();
      Result<SelectResultValue> actual = actualIter.next();

      Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());

      for (Map.Entry<String, Integer> entry : expected.getValue().getPagingIdentifiers().entrySet()) {
        Assert.assertEquals(entry.getValue(), actual.getValue().getPagingIdentifiers().get(entry.getKey()));
      }

      Iterator<EventHolder> expectedEvts = expected.getValue().getEvents().iterator();
      Iterator<EventHolder> actualEvts = actual.getValue().getEvents().iterator();

      while (expectedEvts.hasNext()) {
        EventHolder exHolder = expectedEvts.next();
        EventHolder acHolder = actualEvts.next();

        Assert.assertEquals(exHolder.getTimestamp(), acHolder.getTimestamp());
        Assert.assertEquals(exHolder.getOffset(), acHolder.getOffset());

        for (Map.Entry<String, Object> ex : exHolder.getEvent().entrySet()) {
          Object actVal = acHolder.getEvent().get(ex.getKey());

          // work around for current II limitations
          if (acHolder.getEvent().get(ex.getKey()) instanceof Double) {
            actVal = ((Double) actVal).floatValue();
          }
          Assert.assertEquals(ex.getValue(), actVal);
        }
      }

      if (actualEvts.hasNext()) {
        throw new ISE("This event iterator should be exhausted!");
      }
    }

    if (actualIter.hasNext()) {
      throw new ISE("This iterator should be exhausted!");
    }
  }
}
