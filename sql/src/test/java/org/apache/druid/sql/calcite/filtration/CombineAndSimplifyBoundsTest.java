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
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class CombineAndSimplifyBoundsTest extends BaseCalciteQueryTest
{

  enum RangeFilterType
  {
    BOUND {
      @Override
      public DimFilter range(String lit1, boolean gte, String name, boolean lte, String lit2)
      {
        return new BoundDimFilter(
            name,
            lit1,
            lit2,
            !gte,
            !lte,
            true,
            null,
            null
        );
      }
    },
    RANGE {
      @Override
      public DimFilter range(String lit1, boolean gte, String name, boolean lte, String lit2)
      {
        return new RangeFilter(
            name,
            ColumnType.STRING,
            lit1,
            lit2,
            !gte,
            !lte,
            null
        );
      }
    };

    public final DimFilter lt(String name, String literal)
    {
      return range(null, false, name, false, literal);
    }

    public final DimFilter le(String name, String literal)
    {
      return range(null, false, name, true, literal);
    }

    public final DimFilter gt(String name, String literal)
    {
      return range(literal, false, name, false, null);
    }

    public final DimFilter ge(String name, String literal)
    {
      return range(literal, true, name, false, null);
    }

    public final DimFilter eq(String name, String literal)
    {
      return range(literal, true, name, true, literal);
    }

    public abstract DimFilter range(String lit1, boolean gte, String name, boolean lte, String lit2);
  }

  @Parameters
  public static List<Object[]> getParameters()
  {
    return ImmutableList.of(
        new Object[] {RangeFilterType.BOUND},
        new Object[] {RangeFilterType.RANGE}
    );
  }

  final RangeFilterType rangeFilter;

  public CombineAndSimplifyBoundsTest(RangeFilterType rangeFilter)
  {
    this.rangeFilter = rangeFilter;
  }

  @Test
  public void testNotAZ()
  {
    String dim1 = "dim1";
    DimFilter inputFilter = or(
        rangeFilter.lt(dim1, "a"),
        rangeFilter.gt(dim1, "z")
    );
    DimFilter expected = not(rangeFilter.range("a", true, dim1, true, "z"));

    check(inputFilter, expected);
  }

  @Test
  public void testAZ()
  {
    String dim1 = "dim1";
    DimFilter inputFilter = and(
        rangeFilter.gt(dim1, "a"),
        rangeFilter.lt(dim1, "z")
    );
    DimFilter expected = rangeFilter.range("a", false, dim1, false, "z");

    check(inputFilter, expected);
  }

  @Test
  public void testNot2()
  {
    String dim1 = "dim1";
    DimFilter inputFilter = or(
        rangeFilter.le(dim1, "a"),
        rangeFilter.range("f", true, dim1, false, "j"),
        rangeFilter.gt(dim1, "z")
    );
    DimFilter expected = not(
        or(
            rangeFilter.range("a", false, dim1, false, "f"),
            rangeFilter.range("j", true, dim1, true, "z")
        )
    );

    check(inputFilter, expected);
  }

  private void check(DimFilter inputFilter, DimFilter expected)
  {
    Filtration filt = Filtration.create(inputFilter, null);
    Filtration ret = CombineAndSimplifyBounds.instance().apply(filt);
    assertEquals(expected, ret.getDimFilter());
  }
}
