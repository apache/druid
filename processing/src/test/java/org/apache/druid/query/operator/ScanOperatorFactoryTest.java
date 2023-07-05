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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.DimFilters;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.TestRowsAndColumns;
import org.apache.druid.query.rowsandcols.semantic.RowsAndColumnsDecorator;
import org.apache.druid.query.rowsandcols.semantic.TestRowsAndColumnsDecorator;
import org.apache.druid.segment.VirtualColumns;
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
  public void testSerialization() throws JsonProcessingException
  {
    ObjectMapper mapper = DefaultObjectMapper.INSTANCE;

    ScanOperatorFactory factory = new ScanOperatorFactory(
        Intervals.utc(0, 6),
        DimFilters.dimEquals("abc", "d"),
        20,
        Arrays.asList("dim1", "dim2"),
        VirtualColumns.EMPTY,
        Arrays.asList(ColumnWithDirection.descending("dim2"), ColumnWithDirection.ascending("dim1"))
    );

    final String asString = mapper.writeValueAsString(factory);
    final ScanOperatorFactory deserialized = mapper.readValue(asString, ScanOperatorFactory.class);

    Assert.assertEquals(factory, deserialized);
    Assert.assertEquals(factory.hashCode(), deserialized.hashCode());
  }

  @Test
  public void testWrappedOperatorCarriesThroughValues()
  {
    Interval[] intervals = new Interval[]{Intervals.utc(0, 6), Intervals.utc(6, 13), Intervals.utc(4, 8)};
    DimFilter[] filters = new DimFilter[]{
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
    int[] limits = new int[]{100, 1000};
    List<ColumnWithDirection>[] orderings = new List[]{
        Arrays.asList(ColumnWithDirection.descending("__time"), ColumnWithDirection.ascending("dim")),
        Collections.singletonList(ColumnWithDirection.ascending("val"))
    };

    for (int i = 2; i <= intervals.length; ++i) {
      Interval interval = (i == 0 ? null : intervals[i - 1]);
      for (int j = 0; j <= filters.length; ++j) {
        DimFilter filter = (j == 0 ? null : filters[j - 1]);
        for (int k = 0; k <= limits.length; ++k) {
          int limit = (k == 0 ? -1 : limits[k - 1]);
          for (int l = 0; l <= orderings.length; ++l) {


            ScanOperatorFactory factory = new ScanOperatorFactory(
                interval,
                filter,
                limit,
                Arrays.asList("dim1", "dim2"),
                VirtualColumns.EMPTY,
                Arrays.asList(ColumnWithDirection.descending("dim2"), ColumnWithDirection.ascending("dim1"))
            );

            final ScanOperator wrapped = (ScanOperator) factory.wrap(new Operator()
            {
              @Nullable
              @Override
              public Closeable goOrContinue(
                  Closeable continuationObject, Receiver receiver
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

                    Assert.assertEquals(factory.getTimeRange(), rac.getTimeRange());
                    Assert.assertEquals(factory.getLimit(), rac.getLimit());
                    Assert.assertEquals(factory.getVirtualColumns(), rac.getVirtualColumns());
                    Assert.assertEquals(factory.getOrdering(), rac.getOrdering());
                    Assert.assertEquals(factory.getProjectedColumns(), rac.getProjectedColumns());
                    Assert.assertEquals(
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
        }
      }
    }
  }
}