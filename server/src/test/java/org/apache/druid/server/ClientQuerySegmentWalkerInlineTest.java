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

package org.apache.druid.server;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientQuerySegmentWalkerInlineTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testToInlineDataSourceCanExecuteEmptySubquery()
  {
    RowSignature rowSignature = RowSignature
        .builder()
        .add("col1", ValueType.STRING)
        .add("col2", ValueType.LONG)
        .build();
    // Emulate a query that its subquery is already inlined
    AtomicInteger lineAccumulator = new AtomicInteger(3);
    TestQuery query = new TestQuery(
        InlineDataSource.fromIterable(
            ImmutableList.of(
                new Object[]{"oldVal1", 1},
                new Object[]{"oldVal2", 2},
                new Object[]{"oldVal3", 3}
            ),
            rowSignature
        ),
        rowSignature
    );
    // Emulate a query that processes 3 rows in the inline datasource and creates 0 rows
    ClientQuerySegmentWalker.toInlineDataSource(
        query,
        Sequences.simple(ImmutableList.of()),
        new TestQueryToolChest(),
        lineAccumulator,
        3
    );
    Assert.assertEquals(0, lineAccumulator.get());
  }

  @Test
  public void testToInlineDataSourceAdjustLineAccumulatorAfterSuccess()
  {
    RowSignature rowSignature = RowSignature
        .builder()
        .add("col1", ValueType.STRING)
        .add("col2", ValueType.LONG)
        .build();
    // Emulate a query that its subquery is already inlined
    AtomicInteger lineAccumulator = new AtomicInteger(3);
    TestQuery query = new TestQuery(
        InlineDataSource.fromIterable(
            ImmutableList.of(
                new Object[]{"oldVal1", 1},
                new Object[]{"oldVal2", 2},
                new Object[]{"oldVal3", 3}
            ),
            rowSignature
        ),
        rowSignature
    );
    // Emulate a query that processes 3 rows in the inline datasource and creates 2 rows
    ClientQuerySegmentWalker.toInlineDataSource(
        query,
        Sequences.simple(ImmutableList.of(ResultRow.of("newVal1", 1), ResultRow.of("newVal2", 2))),
        new TestQueryToolChest(),
        lineAccumulator,
        5
    );
    Assert.assertEquals(2, lineAccumulator.get());
  }

  @Test
  public void testToInlineDataSourceHitMaxSubqueryRowsAndFail()
  {
    RowSignature rowSignature = RowSignature
        .builder()
        .add("col1", ValueType.STRING)
        .add("col2", ValueType.LONG)
        .build();
    // Emulate a query that its subquery is already inlined
    AtomicInteger lineAccumulator = new AtomicInteger(3);
    TestQuery query = new TestQuery(
        InlineDataSource.fromIterable(
            ImmutableList.of(
                new Object[]{"oldVal1", 1},
                new Object[]{"oldVal2", 2},
                new Object[]{"oldVal3", 3}
            ),
            rowSignature
        ),
        rowSignature
    );
    // Emulate a query that processes 3 rows in the inline datasource and creates 2 rows.
    // This should fail due to the maxSubqueryRows of 4 because it needs to hold 5 rows during processing the query.
    expectedException.expect(ResourceLimitExceededException.class);
    expectedException.expectMessage("Subquery generated results beyond maximum[4]");
    ClientQuerySegmentWalker.toInlineDataSource(
        query,
        Sequences.simple(ImmutableList.of(ResultRow.of("newVal1", 1), ResultRow.of("newVal2", 2))),
        new TestQueryToolChest(),
        lineAccumulator,
        4
    );
  }

  private static class TestQuery extends BaseQuery<ResultRow>
  {
    private final RowSignature rowSignature;

    private TestQuery(DataSource dataSource, RowSignature rowSignature)
    {
      super(dataSource, Mockito.mock(QuerySegmentSpec.class), false, null);
      this.rowSignature = rowSignature;
    }

    @Override
    public Query<ResultRow> withDataSource(DataSource dataSource)
    {
      return new TestQuery(dataSource, rowSignature);
    }

    @Override
    public boolean hasFilters()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public DimFilter getFilter()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getType()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Query<ResultRow> withOverriddenContext(Map<String, Object> contextOverride)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Query<ResultRow> withQuerySegmentSpec(QuerySegmentSpec spec)
    {
      throw new UnsupportedOperationException();
    }
  }

  private static class TestQueryToolChest extends QueryToolChest<ResultRow, TestQuery>
  {
    @Override
    public RowSignature resultArraySignature(TestQuery query)
    {
      return query.rowSignature;
    }

    @Override
    public Sequence<Object[]> resultsAsArrays(TestQuery query, Sequence<ResultRow> resultSequence)
    {
      return resultSequence.map(ResultRow::getArray);
    }

    @Override
    public QueryMetrics<? super TestQuery> makeMetrics(TestQuery query)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Function<ResultRow, ResultRow> makePreComputeManipulatorFn(TestQuery query, MetricManipulationFn fn)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public TypeReference<ResultRow> getResultTypeReference()
    {
      return new TypeReference<ResultRow>()
      {
      };
    }
  }
}
