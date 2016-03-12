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

package io.druid.query.filter;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.CharSource;
import com.metamx.common.guava.Sequences;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Druids;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.select.EventHolder;
import io.druid.query.select.PagingSpec;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectQueryEngine;
import io.druid.query.select.SelectQueryQueryToolChest;
import io.druid.query.select.SelectQueryRunnerFactory;
import io.druid.query.select.SelectResultValue;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.TestIndex;
import io.druid.segment.incremental.IncrementalIndex;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static io.druid.query.QueryRunnerTestHelper.makeQueryRunner;
import static io.druid.query.QueryRunnerTestHelper.marketDimension;
import static io.druid.query.ordering.StringComparators.ALPHANUMERIC_NAME;
import static io.druid.query.ordering.StringComparators.LEXICOGRAPHIC_NAME;
import static io.druid.query.ordering.StringComparators.NUMERIC_NAME;

/**
 */
@RunWith(Parameterized.class)
public class CompareTypeTest
{
  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    SelectQueryRunnerFactory factory = new SelectQueryRunnerFactory(
        new SelectQueryQueryToolChest(
            new DefaultObjectMapper(),
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        new SelectQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );

    CharSource input = CharSource.wrap(
        "2011-01-12T00:00:00.000Z\t9.45	automotive	preferred	apreferred	100.000000\n" +
        "2011-01-12T00:00:00.000Z\t-1.2	business	preferred	bpreferred	100.000000\n" +
        "2011-01-12T00:00:00.000Z\t4.7	entertainment	preferred	epreferred	100.000000\n" +
        "2011-01-12T00:00:00.000Z\t\thealth	preferred	hpreferred	100.000000\n" +
        "2011-01-12T00:00:00.000Z\t13.45	mezzanine	preferred	mpreferred	100.000000\n"
    );

    IncrementalIndex index1 = TestIndex.makeRealtimeIndex(input);
    IncrementalIndex index2 = TestIndex.makeRealtimeIndex(input);

    QueryableIndex index3 = TestIndex.persistRealtimeAndLoadMMapped(index1);
    QueryableIndex index4 = TestIndex.persistRealtimeAndLoadMMapped(index2);

    return QueryRunnerTestHelper.transformToConstructionFeeder(
        Arrays.asList(
            makeQueryRunner(factory, "index1", new IncrementalIndexSegment(index1, "index1")),
            makeQueryRunner(factory, "index2", new IncrementalIndexSegment(index2, "index2")),
            makeQueryRunner(factory, "index3", new QueryableIndexSegment("index3", index3)),
            makeQueryRunner(factory, "index4", new QueryableIndexSegment("index4", index4))
        )
    );
  }

  private final QueryRunner runner;

  public CompareTypeTest(
      QueryRunner runner
  )
  {
    this.runner = runner;
  }


  @Test
  public void testTimeSeriesWithFilteredAggWithSelectDimFilter()
  {
    Druids.SelectQueryBuilder builder =
        Druids.newSelectQueryBuilder()
              .dimensions(Arrays.asList(QueryRunnerTestHelper.marketDimension))
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .pagingSpec(new PagingSpec(null, 10));

    // lexicographic : [null, -1.2, 13.45, 4.7, 9.45]
    // alphaNumeric : [null, 4.7, 9.45, 13.45, -1.2]
    // numeric : [null, -1.2, 4.7, 9.45, 13.45]

    // existing
    validate(builder, ">", "4.7", "[9.45]", "[-1.2, 13.45, 9.45]", "[13.45, 9.45]");
    validate(builder, ">=", "4.7", "[4.7, 9.45]", "[-1.2, 13.45, 4.7, 9.45]", "[13.45, 4.7, 9.45]");
    validate(builder, "<", "4.7", "[null, -1.2, 13.45]", "[null]", "[null, -1.2]");
    validate(builder, "<=", "4.7", "[null, -1.2, 13.45, 4.7]", "[null, 4.7]", "[null, -1.2, 4.7]");
    validate(builder, "==", "4.7", "[4.7]", "[4.7]", "[4.7]");
    validate(
        builder, "<>", "4.7",
        "[null, -1.2, 13.45, 9.45]", "[null, -1.2, 13.45, 9.45]", "[null, -1.2, 13.45, 9.45]"
    );

    // non-existing (between)
    validate(builder, ">", "3.1", "[4.7, 9.45]", "[-1.2, 13.45, 4.7, 9.45]", "[13.45, 4.7, 9.45]");
    validate(builder, ">=", "3.1", "[4.7, 9.45]", "[-1.2, 13.45, 4.7, 9.45]", "[13.45, 4.7, 9.45]");
    validate(builder, "<", "3.1", "[null, -1.2, 13.45]", "[null]", "[null, -1.2]");
    validate(builder, "<=", "3.1", "[null, -1.2, 13.45]", "[null]", "[null, -1.2]");
    validate(builder, "==", "3.1", "[]", "[]", "[]");
    validate(
        builder, "<>", "3.1",
        "[null, -1.2, 13.45, 4.7, 9.45]", "[null, -1.2, 13.45, 4.7, 9.45]", "[null, -1.2, 13.45, 4.7, 9.45]"
    );

    // non-existing (smaller than min)
    validate(builder, ">", "-12.4", "[13.45, 4.7, 9.45]", "[]", "[-1.2, 13.45, 4.7, 9.45]");
    validate(builder, ">=", "-12.4", "[13.45, 4.7, 9.45]", "[]", "[-1.2, 13.45, 4.7, 9.45]");
    validate(builder, "<", "-12.4", "[null, -1.2]", "[null, -1.2, 13.45, 4.7, 9.45]", "[null]");
    validate(builder, "<=", "-12.4", "[null, -1.2]", "[null, -1.2, 13.45, 4.7, 9.45]", "[null]");
    validate(builder, "==", "-12.4", "[]", "[]", "[]");
    validate(
        builder, "<>", "-12.4",
        "[null, -1.2, 13.45, 4.7, 9.45]", "[null, -1.2, 13.45, 4.7, 9.45]", "[null, -1.2, 13.45, 4.7, 9.45]"
    );

    // non-existing (bigger than max)
    validate(builder, ">", "42", "[9.45]", "[-1.2]", "[]");
    validate(builder, ">=", "42", "[9.45]", "[-1.2]", "[]");
    validate(
        builder, "<", "42",
        "[null, -1.2, 13.45, 4.7]", "[null, 13.45, 4.7, 9.45]", "[null, -1.2, 13.45, 4.7, 9.45]"
    );
    validate(
        builder, "<=", "42",
        "[null, -1.2, 13.45, 4.7]", "[null, 13.45, 4.7, 9.45]", "[null, -1.2, 13.45, 4.7, 9.45]"
    );
    validate(builder, "==", "42", "[]", "[]", "[]");
    validate(
        builder, "<>", "42",
        "[null, -1.2, 13.45, 4.7, 9.45]", "[null, -1.2, 13.45, 4.7, 9.45]", "[null, -1.2, 13.45, 4.7, 9.45]"
    );

    // null
    validate(builder, "==", "", "[null]", "[null]", "[null]");
    validate(builder, "<>", "", "[-1.2, 13.45, 4.7, 9.45]", "[-1.2, 13.45, 4.7, 9.45]", "[-1.2, 13.45, 4.7, 9.45]");
  }

  private void validate(
      Druids.SelectQueryBuilder builder,
      String operation,
      String value,
      String lexicographicExpected,
      String alphaNumericExpected,
      String numericExpected
  )
  {
    builder.filters(new SelectorDimFilterExtension(marketDimension, value, operation, LEXICOGRAPHIC_NAME));
    validate(builder.build(), lexicographicExpected);

    builder.filters(new SelectorDimFilterExtension(marketDimension, value, operation, ALPHANUMERIC_NAME));
    validate(builder.build(), alphaNumericExpected);

    builder.filters(new SelectorDimFilterExtension(marketDimension, value, operation, NUMERIC_NAME));
    validate(builder.build(), numericExpected);
  }

  private void validate(SelectQuery query, String expected)
  {
    Iterable<Result<SelectResultValue>> results = Sequences.toList(
        runner.run(query, ImmutableMap.of()),
        Lists.<Result<SelectResultValue>>newArrayList()
    );
    Iterator<Result<SelectResultValue>> iterator = results.iterator();
    Assert.assertTrue(iterator.hasNext());
    List<EventHolder> events = iterator.next().getValue().getEvents();
    List<String> markets = Lists.transform(
        events, new Function<EventHolder, String>()
        {
          @Override
          public String apply(EventHolder input)
          {
            return (String) input.getEvent().get(marketDimension);
          }
        }
    );
    Assert.assertEquals(expected, markets.toString());
    Assert.assertFalse(iterator.hasNext());
  }

}
