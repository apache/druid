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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequences;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Druids;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.select.PagingSpec;
import io.druid.query.select.SelectQueryEngine;
import io.druid.query.select.SelectQueryQueryToolChest;
import io.druid.query.select.SelectQueryRunnerFactory;
import io.druid.query.select.SelectQueryRunnerTest;
import io.druid.query.select.SelectResultValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;

/**
 */
@RunWith(Parameterized.class)
public class SelectQueryTest
{

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

  public SelectQueryTest(QueryRunner runner, boolean descending)
  {
    this.runner = runner;
    this.descending = descending;
  }

  @Test
  public void testSelectWithSelectDimFilter()
  {
    Druids.SelectQueryBuilder builder = new Druids.SelectQueryBuilder()
        .dataSource(new TableDataSource(QueryRunnerTestHelper.dataSource))
        .intervals(SelectQueryRunnerTest.I_0112_0114)
        .descending(descending)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimensionSpecs(DefaultDimensionSpec.toSpec(Arrays.asList(QueryRunnerTestHelper.qualityDimension)))
        .metrics(Arrays.asList(QueryRunnerTestHelper.indexMetric))
        .pagingSpec(new PagingSpec(null, 20));

    // existing
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "entertainment", "GT"),
        new JavaScriptDimFilter(
            QueryRunnerTestHelper.qualityDimension,
            "function(dim){ return dim > 'entertainment'; }"
        )
    );
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "entertainment", "GTE"),
        new JavaScriptDimFilter(
            QueryRunnerTestHelper.qualityDimension,
            "function(dim){ return dim >= 'entertainment'; }"
        )
    );
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "entertainment", "LT"),
        new JavaScriptDimFilter(
            QueryRunnerTestHelper.qualityDimension,
            "function(dim){ return dim < 'entertainment'; }"
        )
    );
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "entertainment", "LTE"),
        new JavaScriptDimFilter(
            QueryRunnerTestHelper.qualityDimension,
            "function(dim){ return dim <= 'entertainment'; }"
        )
    );
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "entertainment", "EQ"),
        new JavaScriptDimFilter(
            QueryRunnerTestHelper.qualityDimension,
            "function(dim){ return dim === 'entertainment'; }"
        )
    );
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "entertainment", "NE"),
        new JavaScriptDimFilter(
            QueryRunnerTestHelper.qualityDimension,
            "function(dim){ return dim != 'entertainment'; }"
        )
    );

    // non-existing (between)
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "financial", "GT"),
        new JavaScriptDimFilter(QueryRunnerTestHelper.qualityDimension, "function(dim){ return dim > 'financial'; }")
    );
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "financial", "GTE"),
        new JavaScriptDimFilter(QueryRunnerTestHelper.qualityDimension, "function(dim){ return dim >= 'financial'; }")
    );
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "financial", "LT"),
        new JavaScriptDimFilter(QueryRunnerTestHelper.qualityDimension, "function(dim){ return dim < 'financial'; }")
    );
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "financial", "LTE"),
        new JavaScriptDimFilter(QueryRunnerTestHelper.qualityDimension, "function(dim){ return dim <= 'financial'; }")
    );
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "financial", "EQ"),
        new JavaScriptDimFilter(QueryRunnerTestHelper.qualityDimension, "function(dim){ return dim === 'financial'; }")
    );
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "financial", "NE"),
        new JavaScriptDimFilter(QueryRunnerTestHelper.qualityDimension, "function(dim){ return dim != 'financial'; }")
    );

    // non-existing (smaller than min)
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "abcb", "GT"),
        new JavaScriptDimFilter(QueryRunnerTestHelper.qualityDimension, "function(dim){ return dim > 'abcb'; }")
    );
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "abcb", "GTE"),
        new JavaScriptDimFilter(QueryRunnerTestHelper.qualityDimension, "function(dim){ return dim >= 'abcb'; }")
    );
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "abcb", "LT"),
        new JavaScriptDimFilter(QueryRunnerTestHelper.qualityDimension, "function(dim){ return dim < 'abcb'; }")
    );
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "abcb", "LTE"),
        new JavaScriptDimFilter(QueryRunnerTestHelper.qualityDimension, "function(dim){ return dim <= 'abcb'; }")
    );
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "abcb", "EQ"),
        new JavaScriptDimFilter(QueryRunnerTestHelper.qualityDimension, "function(dim){ return dim === 'abcb'; }")
    );
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "abcb", "NE"),
        new JavaScriptDimFilter(QueryRunnerTestHelper.qualityDimension, "function(dim){ return dim != 'abcb'; }")
    );

    // non-existing (bigger than max)
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "zztop", "GT"),
        new JavaScriptDimFilter(QueryRunnerTestHelper.qualityDimension, "function(dim){ return dim > 'zztop'; }")
    );
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "zztop", "GTE"),
        new JavaScriptDimFilter(QueryRunnerTestHelper.qualityDimension, "function(dim){ return dim >= 'zztop'; }")
    );
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "zztop", "LT"),
        new JavaScriptDimFilter(QueryRunnerTestHelper.qualityDimension, "function(dim){ return dim < 'zztop'; }")
    );
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "zztop", "LTE"),
        new JavaScriptDimFilter(QueryRunnerTestHelper.qualityDimension, "function(dim){ return dim <= 'zztop'; }")
    );
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "zztop", "EQ"),
        new JavaScriptDimFilter(QueryRunnerTestHelper.qualityDimension, "function(dim){ return dim === 'zztop'; }")
    );
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension(QueryRunnerTestHelper.qualityDimension, "zztop", "NE"),
        new JavaScriptDimFilter(QueryRunnerTestHelper.qualityDimension, "function(dim){ return dim != 'zztop'; }")
    );

    // null
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension("partial_null_column", "", "EQ"),
        new JavaScriptDimFilter("partial_null_column", "function(dim){ return dim === null; }")
    );
    validateSelectDimFilter(
        builder,
        new SelectorDimFilterExtension("partial_null_column", "", "NE"),
        new JavaScriptDimFilter("partial_null_column", "function(dim){ return dim != null; }")
    );
  }

  private void validateSelectDimFilter(
      Druids.SelectQueryBuilder builder,
      SelectorDimFilterExtension selectorFilter,
      JavaScriptDimFilter javaScriptDimFilter
  )
  {

    Iterable<Result<SelectResultValue>> expected = Sequences.toList(
        runner.run(builder.filters(javaScriptDimFilter).build(), ImmutableMap.of()),
        Lists.<Result<SelectResultValue>>newArrayList()
    );
    Iterable<Result<SelectResultValue>> results = Sequences.toList(
        runner.run(builder.filters(selectorFilter).build(), ImmutableMap.of()),
        Lists.<Result<SelectResultValue>>newArrayList()
    );
    SelectQueryRunnerTest.verify(expected, results);
  }
}
