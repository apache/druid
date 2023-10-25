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

package org.apache.druid.query.operator;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.DimFilters;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.TestRowsAndColumns;
import org.apache.druid.query.rowsandcols.semantic.RowsAndColumnsDecorator;
import org.apache.druid.query.rowsandcols.semantic.TestRowsAndColumnsDecorator;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("unchecked")
public class ScanOperatorFactoryTest
{
  static {
    NullHandling.initializeForTests();
  }

  @Test
  public void testEquals()
  {
    final Builder bob = new Builder();
    bob.timeRange = Intervals.utc(0, 6);
    bob.filter = DimFilters.dimEquals("abc", "b");
    bob.offsetLimit = OffsetLimit.limit(48);
    bob.projectedColumns = Arrays.asList("a", "b");
    bob.virtualColumns = VirtualColumns.EMPTY;
    bob.ordering = Collections.singletonList(ColumnWithDirection.ascending("a"));
    ScanOperatorFactory factory = bob.build();

    Assert.assertEquals(factory, factory);
    Assert.assertNotEquals(factory, new Object());

    Assert.assertNotEquals(factory, bob.copy().setTimeRange(null).build());
    Assert.assertNotEquals(factory, bob.copy().setFilter(null).build());
    Assert.assertNotEquals(factory, bob.copy().setOffsetLimit(null).build());
    Assert.assertNotEquals(factory, bob.copy().setProjectedColumns(null).build());
    Assert.assertNotEquals(factory, bob.copy().setVirtualColumns(null).build());
    Assert.assertNotEquals(factory, bob.copy().setOrdering(null).build());
  }

  @Test
  public void testWrappedOperatorCarriesThroughValues() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(new InjectableValues.Std().addValue(ExprMacroTable.class, TestExprMacroTable.INSTANCE));

    Interval[] intervals = new Interval[]{null, Intervals.utc(0, 6), Intervals.utc(6, 13), Intervals.utc(4, 8)};
    DimFilter[] filters = new DimFilter[]{
        null,
        new InDimFilter("dim", ImmutableSet.of("a", "b", "c", "e", "g")),
        DimFilters.and(
            new InDimFilter("dim", ImmutableSet.of("a", "b", "g")),
            DimFilters.dimEquals("val", "789")
        ),
        DimFilters.or(
            DimFilters.dimEquals("dim", "b"),
            DimFilters.dimEquals("val", "789")
        ),
        DimFilters.dimEquals("dim", "f")
    };
    int[] offsets = new int[]{0, 10};
    int[] limits = new int[]{-1, 100, 1000};
    List<ColumnWithDirection>[] orderings = new List[]{
        null,
        Arrays.asList(ColumnWithDirection.descending("__time"), ColumnWithDirection.ascending("dim")),
        Collections.singletonList(ColumnWithDirection.ascending("val")),
        Collections.emptyList()
    };
    List<String>[] projections = new List[]{
        null,
        Arrays.asList("dim", "val"),
        Collections.singletonList("dim"),
        Collections.emptyList()
    };
    VirtualColumns[] virtualCols = new VirtualColumns[]{
        null,
        VirtualColumns.EMPTY,
        VirtualColumns.create(Collections.singletonList(
            new ExpressionVirtualColumn("test", "2", null, TestExprMacroTable.INSTANCE)
        ))
    };

    for (Interval interval : intervals) {
      for (DimFilter filter : filters) {
        for (int offset : offsets) {
          for (int limit : limits) {
            for (List<ColumnWithDirection> ordering : orderings) {
              for (List<String> projection : projections) {
                for (VirtualColumns virtual : virtualCols) {

                  String msg = StringUtils.format(
                      "interval[%s], filter[%s], limit[%s], ordering[%s], projection[%s], virtual[%s]",
                      interval,
                      filter,
                      OffsetLimit.build(offset, limit),
                      ordering,
                      projection,
                      virtual);

                  ScanOperatorFactory factory = new ScanOperatorFactory(
                      interval,
                      filter,
                      OffsetLimit.build(offset, limit),
                      projection,
                      virtual,
                      ordering);

                  testOperatorFactoryInternal(mapper, msg, factory);
                }
              }
            }
          }
        }
      }
    }
  }

  private void testOperatorFactoryInternal(ObjectMapper mapper, String msg, ScanOperatorFactory factory)
      throws Exception
  {
    final String asString = mapper.writeValueAsString(factory);
    final ScanOperatorFactory deserialized = mapper.readValue(asString, ScanOperatorFactory.class);

    Assert.assertEquals(msg, factory, deserialized);
    Assert.assertEquals(msg, factory.hashCode(), deserialized.hashCode());

    final ScanOperator wrapped = (ScanOperator) factory.wrap(new Operator()
    {
      @Nullable
      @Override
      public Closeable goOrContinue(
          Closeable continuationObject,
          Receiver receiver
      )
      {
        receiver.push(new TestRowsAndColumns().withAsImpl(
            RowsAndColumnsDecorator.class,
            TestRowsAndColumnsDecorator::new
        ));
        receiver.completed();
        return null;
      }
    });

    Operator.go(
        wrapped,
        new Operator.Receiver()
        {
          @Override
          public Operator.Signal push(RowsAndColumns inRac)
          {
            TestRowsAndColumnsDecorator.DecoratedRowsAndColumns rac =
                (TestRowsAndColumnsDecorator.DecoratedRowsAndColumns) inRac;

            Assert.assertEquals(msg, factory.getTimeRange(), rac.getTimeRange());
            Assert.assertEquals(msg, factory.getOffsetLimit(), rac.getLimit());
            Assert.assertEquals(msg, factory.getVirtualColumns(), rac.getVirtualColumns());
            validateList(msg, factory.getOrdering(), rac.getOrdering());
            validateList(msg, factory.getProjectedColumns(), rac.getProjectedColumns());

            Assert.assertEquals(
                msg,
                factory.getFilter() == null ? null : factory.getFilter().toFilter(),
                rac.getFilter()
            );

            return Operator.Signal.GO;
          }

          @Override
          public void completed()
          {

          }
        }
    );
  }

  private static <T> void validateList(
      String msg,
      List<T> expectedList,
      List<T> actualList
  )
  {
    if (expectedList != null && expectedList.isEmpty()) {
      Assert.assertNull(msg, actualList);
    } else {
      Assert.assertEquals(msg, expectedList, actualList);
    }
  }

  private static class Builder
  {
    private Interval timeRange;
    private DimFilter filter;
    private OffsetLimit offsetLimit;
    private List<String> projectedColumns;
    private VirtualColumns virtualColumns;
    private List<ColumnWithDirection> ordering;

    public Builder setTimeRange(Interval timeRange)
    {
      this.timeRange = timeRange;
      return this;
    }

    public Builder setFilter(DimFilter filter)
    {
      this.filter = filter;
      return this;
    }

    public Builder setOffsetLimit(OffsetLimit limit)
    {
      this.offsetLimit = limit;
      return this;
    }

    public Builder setProjectedColumns(List<String> projectedColumns)
    {
      this.projectedColumns = projectedColumns;
      return this;
    }

    public Builder setVirtualColumns(VirtualColumns virtualColumns)
    {
      this.virtualColumns = virtualColumns;
      return this;
    }

    public Builder setOrdering(List<ColumnWithDirection> ordering)
    {
      this.ordering = ordering;
      return this;
    }

    private Builder copy()
    {
      Builder retVal = new Builder();
      retVal.timeRange = timeRange;
      retVal.filter = filter;
      retVal.offsetLimit = offsetLimit;
      retVal.projectedColumns = projectedColumns;
      retVal.virtualColumns = virtualColumns;
      retVal.ordering = ordering;
      return retVal;
    }

    private ScanOperatorFactory build()
    {
      return new ScanOperatorFactory(
          timeRange,
          filter,
          offsetLimit,
          projectedColumns,
          virtualColumns,
          ordering
      );
    }
  }
}
