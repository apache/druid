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

package org.apache.druid.sql.calcite.filtration;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.IntervalDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class FiltrationTest extends CalciteTestBase
{
  @Test
  public void testNotIntervals()
  {
    final Filtration filtration = Filtration.create(
        new NotDimFilter(
            new IntervalDimFilter(
                ColumnHolder.TIME_COLUMN_NAME,
                ImmutableList.of(Intervals.of("2000/2001"), Intervals.of("2002/2003")),
                null
            )
        ),
        null
    ).optimize(RowSignature.builder().add(ColumnHolder.TIME_COLUMN_NAME, ColumnType.LONG).build());

    Assertions.assertEquals(
        ImmutableList.of(Filtration.eternity()),
        filtration.getIntervals()
    );

    Assertions.assertEquals(
        new NotDimFilter(
            new IntervalDimFilter(
                ColumnHolder.TIME_COLUMN_NAME,
                ImmutableList.of(Intervals.of("2000/2001"), Intervals.of("2002/2003")),
                null
            )
        ),
        filtration.getDimFilter()
    );
  }

  @Test
  public void testComplexEqualitiesRemainOr()
  {
    TypeStrategies.registerComplex(ColumnType.NESTED_DATA.getComplexTypeName(), TypeStrategies.NESTED);
    final RowSignature signature = RowSignature.builder().add("json", ColumnType.NESTED_DATA).build();
    final OrDimFilter filter = new OrDimFilter(
        ImmutableList.of(
            new EqualityFilter("json", ColumnType.NESTED_DATA, Map.of("x", 1L), null),
            new EqualityFilter("json", ColumnType.NESTED_DATA, Map.of("x", 2L), null)
        )
    );
    Assertions.assertEquals(filter, Filtration.create(filter).optimize(signature).getDimFilter());
  }
}
