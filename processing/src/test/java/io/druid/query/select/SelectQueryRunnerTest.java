/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query.select;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class SelectQueryRunnerTest
{
  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.transformToConstructionFeeder(
        QueryRunnerTestHelper.makeQueryRunners(
            new SelectQueryRunnerFactory(
                new SelectQueryQueryToolChest(
                    new DefaultObjectMapper(),
                    QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                ),
                new SelectQueryEngine(),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        )
    );
  }

  private static final String providerLowercase = "market";

  private final QueryRunner runner;

  public SelectQueryRunnerTest(
      QueryRunner runner
  )
  {
    this.runner = runner;
  }

  @Test
  public void testFullOnSelect()
  {
    SelectQuery query = new SelectQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        QueryRunnerTestHelper.fullOnInterval,
        null,
        QueryRunnerTestHelper.allGran,
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        new PagingSpec(null, 3),
        null
    );
    HashMap<String,Object> context = new HashMap<String, Object>();
    Iterable<Result<SelectResultValue>> results = Sequences.toList(
        runner.run(query, context),
        Lists.<Result<SelectResultValue>>newArrayList()
    );

    List<Result<SelectResultValue>> expectedResults = Arrays.asList(
        new Result<SelectResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SelectResultValue(
                ImmutableMap.of(QueryRunnerTestHelper.segmentId, 2),
                Arrays.asList(
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        0,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.timestampKey, new DateTime("2011-01-12T00:00:00.000Z"))
                            .put(providerLowercase, "spot")
                            .put(QueryRunnerTestHelper.qualityDimension, "automotive")
                            .put(QueryRunnerTestHelper.placementDimension, "preferred")
                            .put(QueryRunnerTestHelper.placementishDimension, Lists.newArrayList("a", "preferred"))
                            .put(QueryRunnerTestHelper.indexMetric, 100.000000F)
                            .build()
                    ),
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        1,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.timestampKey, new DateTime("2011-01-12T00:00:00.000Z"))
                            .put(providerLowercase, "spot")
                            .put(QueryRunnerTestHelper.qualityDimension, "business")
                            .put(QueryRunnerTestHelper.placementDimension, "preferred")
                            .put(QueryRunnerTestHelper.placementishDimension, Lists.newArrayList("b", "preferred"))
                            .put(QueryRunnerTestHelper.indexMetric, 100.000000F)
                            .build()
                    ),
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        2,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.timestampKey, new DateTime("2011-01-12T00:00:00.000Z"))
                            .put(providerLowercase, "spot")
                            .put(QueryRunnerTestHelper.qualityDimension, "entertainment")
                            .put(QueryRunnerTestHelper.placementDimension, "preferred")
                            .put(QueryRunnerTestHelper.placementishDimension, Lists.newArrayList("e", "preferred"))
                            .put(QueryRunnerTestHelper.indexMetric, 100.000000F)
                            .build()
                    )
                )
            )
        )
    );

    verify(expectedResults, results);
  }

  @Test
  public void testSelectWithDimsAndMets()
  {
    SelectQuery query = new SelectQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        QueryRunnerTestHelper.fullOnInterval,
        null,
        QueryRunnerTestHelper.allGran,
        Lists.<String>newArrayList(providerLowercase),
        Lists.<String>newArrayList(QueryRunnerTestHelper.indexMetric),
        new PagingSpec(null, 3),
        null
    );
    HashMap<String,Object> context = new HashMap<String, Object>();
    Iterable<Result<SelectResultValue>> results = Sequences.toList(
        runner.run(query, context),
        Lists.<Result<SelectResultValue>>newArrayList()
    );

    List<Result<SelectResultValue>> expectedResults = Arrays.asList(
        new Result<SelectResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SelectResultValue(
                ImmutableMap.of(QueryRunnerTestHelper.segmentId, 2),
                Arrays.asList(
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        0,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.timestampKey, new DateTime("2011-01-12T00:00:00.000Z"))
                            .put(providerLowercase, "spot")
                            .put(QueryRunnerTestHelper.indexMetric, 100.000000F)
                            .build()
                    ),
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        1,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.timestampKey, new DateTime("2011-01-12T00:00:00.000Z"))
                            .put(providerLowercase, "spot")
                            .put(QueryRunnerTestHelper.indexMetric, 100.000000F)
                            .build()
                    ),
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        2,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.timestampKey, new DateTime("2011-01-12T00:00:00.000Z"))
                            .put(providerLowercase, "spot")
                            .put(QueryRunnerTestHelper.indexMetric, 100.000000F)
                            .build()
                    )
                )
            )
        )
    );

    verify(expectedResults, results);
  }

  @Test
  public void testSelectPagination()
  {
    SelectQuery query = new SelectQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        QueryRunnerTestHelper.fullOnInterval,
        null,
        QueryRunnerTestHelper.allGran,
        Lists.<String>newArrayList(QueryRunnerTestHelper.qualityDimension),
        Lists.<String>newArrayList(QueryRunnerTestHelper.indexMetric),
        new PagingSpec(Maps.newLinkedHashMap(ImmutableMap.of(QueryRunnerTestHelper.segmentId, 3)), 3),
        null
    );
    HashMap<String,Object> context = new HashMap<String, Object>();
    Iterable<Result<SelectResultValue>> results = Sequences.toList(
        runner.run(query, context),
        Lists.<Result<SelectResultValue>>newArrayList()
    );

    List<Result<SelectResultValue>> expectedResults = Arrays.asList(
        new Result<SelectResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SelectResultValue(
                ImmutableMap.of(QueryRunnerTestHelper.segmentId, 5),
                Arrays.asList(
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        3,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.timestampKey, new DateTime("2011-01-12T00:00:00.000Z"))
                            .put(QueryRunnerTestHelper.qualityDimension, "health")
                            .put(QueryRunnerTestHelper.indexMetric, 100.000000F)
                            .build()
                    ),
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        4,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.timestampKey, new DateTime("2011-01-12T00:00:00.000Z"))
                            .put(QueryRunnerTestHelper.qualityDimension, "mezzanine")
                            .put(QueryRunnerTestHelper.indexMetric, 100.000000F)
                            .build()
                    ),
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        5,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.timestampKey, new DateTime("2011-01-12T00:00:00.000Z"))
                            .put(QueryRunnerTestHelper.qualityDimension, "news")
                            .put(QueryRunnerTestHelper.indexMetric, 100.000000F)
                            .build()
                    )
                )
            )
        )
    );

    verify(expectedResults, results);
  }

  @Test
  public void testFullOnSelectWithFilter()
  {
    SelectQuery query = new SelectQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        new LegacySegmentSpec(new Interval("2011-01-12/2011-01-14")),
        new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "spot"),
        QueryRunnerTestHelper.dayGran,
        Lists.<String>newArrayList(QueryRunnerTestHelper.qualityDimension),
        Lists.<String>newArrayList(QueryRunnerTestHelper.indexMetric),
        new PagingSpec(Maps.newLinkedHashMap(ImmutableMap.of(QueryRunnerTestHelper.segmentId, 3)), 3),
        null
    );
    HashMap<String,Object> context = new HashMap<String, Object>();
    Iterable<Result<SelectResultValue>> results = Sequences.toList(
        runner.run(query, context),
        Lists.<Result<SelectResultValue>>newArrayList()
    );

    List<Result<SelectResultValue>> expectedResults = Arrays.asList(
        new Result<SelectResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SelectResultValue(
                ImmutableMap.of(QueryRunnerTestHelper.segmentId, 5),
                Arrays.asList(
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        3,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.timestampKey, new DateTime("2011-01-12T00:00:00.000Z"))
                            .put(QueryRunnerTestHelper.qualityDimension, "health")
                            .put(QueryRunnerTestHelper.indexMetric, 100.000000F)
                            .build()
                    ),
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        4,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.timestampKey, new DateTime("2011-01-12T00:00:00.000Z"))
                            .put(QueryRunnerTestHelper.qualityDimension, "mezzanine")
                            .put(QueryRunnerTestHelper.indexMetric, 100.000000F)
                            .build()
                    ),
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        5,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.timestampKey, new DateTime("2011-01-12T00:00:00.000Z"))
                            .put(QueryRunnerTestHelper.qualityDimension, "news")
                            .put(QueryRunnerTestHelper.indexMetric, 100.000000F)
                            .build()
                    )
                )
            )
        ),
        new Result<SelectResultValue>(
            new DateTime("2011-01-13T00:00:00.000Z"),
            new SelectResultValue(
                ImmutableMap.of(QueryRunnerTestHelper.segmentId, 5),
                Arrays.asList(
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        3,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.timestampKey, new DateTime("2011-01-13T00:00:00.000Z"))
                            .put(QueryRunnerTestHelper.qualityDimension, "health")
                            .put(QueryRunnerTestHelper.indexMetric, 114.947403F)
                            .build()
                    ),
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        4,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.timestampKey, new DateTime("2011-01-13T00:00:00.000Z"))
                            .put(QueryRunnerTestHelper.qualityDimension, "mezzanine")
                            .put(QueryRunnerTestHelper.indexMetric, 104.465767F)
                            .build()
                    ),
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        5,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.timestampKey, new DateTime("2011-01-13T00:00:00.000Z"))
                            .put(QueryRunnerTestHelper.qualityDimension, "news")
                            .put(QueryRunnerTestHelper.indexMetric, 102.851683F)
                            .build()
                    )
                )
            )
        )
    );

    verify(expectedResults, results);
  }

  @Test
  public void testFullSelectNoResults()
  {
    SelectQuery query = new SelectQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        new LegacySegmentSpec(new Interval("2011-01-12/2011-01-14")),
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
        new LegacySegmentSpec(new Interval("2011-01-12/2011-01-14")),
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

    Map<String, Object> res = Maps.newHashMap();
    res.put("timestamp", new DateTime("2011-01-12T00:00:00.000Z"));
    res.put("foo", null);
    res.put("foo2", null);

    List<Result<SelectResultValue>> expectedResults = Arrays.asList(
        new Result<SelectResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SelectResultValue(
                ImmutableMap.of(QueryRunnerTestHelper.segmentId, 2),
                Arrays.asList(
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        0,
                        res
                    ),
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        1,
                        res
                    ),
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        2,
                        res
                    )
                )
            )
        )
    );

    verify(expectedResults, results);
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
