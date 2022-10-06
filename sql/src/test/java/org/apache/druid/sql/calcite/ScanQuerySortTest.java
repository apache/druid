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

package org.apache.druid.sql.calcite;


import com.google.common.collect.ImmutableList;
import org.apache.druid.query.Druids;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Test;

import java.util.List;

public class ScanQuerySortTest extends BaseCalciteQueryTest
{

  @Test
  public void testScanOrderByLimit()
  {
    //Note: set parameters -Ddruid.generic.useDefaultValueForNull=true/false
    List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{""},
        new Object[]{"1"},
        new Object[]{"10.1"},
        new Object[]{"2"},
        new Object[]{"abc"},
        new Object[]{"def"}
    );

    if ("true".equals(System.getProperty("druid.generic.useDefaultValueForNull", "true"))) {
      expectedResults = ImmutableList.of(
          new Object[]{"1"},
          new Object[]{"10.1"},
          new Object[]{"2"},
          new Object[]{"abc"},
          new Object[]{"def"}
      );
    }

    testQuery(
        "SELECT dim1 FROM druid.foo where dim1 IS NOT NULL ORDER BY dim1 limit 100",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(BaseCalciteQueryTest.querySegmentSpec(Filtration.eternity()))
                  .columns("dim1")
                  .orderBy(ImmutableList.of(new ScanQuery.OrderBy("dim1", ScanQuery.Order.ASCENDING)))
                  .filters(new NotDimFilter(new SelectorDimFilter("dim1", null, null)))
                  .limit(100)
                  .legacy(false)
                  .context(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .build()
        ),
        expectedResults
    );
  }

  @Test
  public void testScanOrderByLimitEmptyResultSet()
  {
    List<Object[]> expectedResults = ImmutableList.of();

    testQuery(
        "SELECT dim1 FROM druid.foo where dim1='xxx' ORDER BY dim1 limit 100",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(BaseCalciteQueryTest.querySegmentSpec(Filtration.eternity()))
                  .columns("dim1")
                  .orderBy(ImmutableList.of(new ScanQuery.OrderBy("dim1", ScanQuery.Order.ASCENDING)))
                  .filters(new SelectorDimFilter("dim1", "xxx", null))
                  .limit(100)
                  .legacy(false)
                  .context(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .build()
        ),
        expectedResults
    );
  }

  @Test
  public void testScanOrderByMoreColumnsLimit()
  {
    //Note: set parameters -Ddruid.generic.useDefaultValueForNull=true/false
    List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{"", "a"},
        new Object[]{"1", "a"},
        new Object[]{"10.1", null},
        new Object[]{"2", ""},
        new Object[]{"abc", null},
        new Object[]{"def", "abc"}
    );

    if ("true".equals(System.getProperty("druid.generic.useDefaultValueForNull", "true"))) {
      expectedResults = ImmutableList.of(
          new Object[]{"", "a"},
          new Object[]{"1", "a"},
          new Object[]{"10.1", ""},
          new Object[]{"2", ""},
          new Object[]{"abc", ""},
          new Object[]{"def", "abc"}
      );
    }

    testQuery(
        "SELECT dim1,dim2 FROM druid.foo  ORDER BY dim1,dim2 limit 100",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(BaseCalciteQueryTest.querySegmentSpec(Filtration.eternity()))
                  .columns("dim1", "dim2")
                  .orderBy(ImmutableList.of(
                      new ScanQuery.OrderBy("dim1", ScanQuery.Order.ASCENDING),
                      new ScanQuery.OrderBy("dim2", ScanQuery.Order.ASCENDING)
                  ))
                  .limit(100)
                  .legacy(false)
                  .context(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .build()
        ),
        expectedResults
    );
  }

  @Test
  public void testScanOrderByAscDescLimit()
  {
    //Note: set parameters -Ddruid.generic.useDefaultValueForNull=true/false
    List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{null, "abc"},
        new Object[]{null, "10.1"},
        new Object[]{"", "2"},
        new Object[]{"a", "1"},
        new Object[]{"a", ""},
        new Object[]{"abc", "def"}
    );

    if ("true".equals(System.getProperty("druid.generic.useDefaultValueForNull", "true"))) {
      expectedResults = ImmutableList.of(
          new Object[]{"", "abc"},
          new Object[]{"", "2"},
          new Object[]{"", "10.1"},
          new Object[]{"a", "1"},
          new Object[]{"a", ""},
          new Object[]{"abc", "def"}
      );
    }

    testQuery(
        "SELECT dim2,dim1 FROM druid.foo  ORDER BY dim2,dim1 desc limit 100",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(BaseCalciteQueryTest.querySegmentSpec(Filtration.eternity()))
                  .columns("dim1", "dim2")
                  .orderBy(ImmutableList.of(new ScanQuery.OrderBy("dim2", ScanQuery.Order.ASCENDING),
                                            new ScanQuery.OrderBy("dim1", ScanQuery.Order.DESCENDING)))
                  .limit(100)
                  .legacy(false)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .build()
        ),
        expectedResults
    );
  }


  @Test
  public void testScanOrderByLimitDesc()
  {
    //Note: set parameters -Ddruid.generic.useDefaultValueForNull=true/false
    List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{"def"},
        new Object[]{"abc"},
        new Object[]{"2"},
        new Object[]{"10.1"},
        new Object[]{"1"},
        new Object[]{""}
    );

    if ("true".equals(System.getProperty("druid.generic.useDefaultValueForNull", "true"))) {
      expectedResults = ImmutableList.of(
          new Object[]{"def"},
          new Object[]{"abc"},
          new Object[]{"2"},
          new Object[]{"10.1"},
          new Object[]{"1"}
      );
    }

    testQuery(
        "SELECT dim1 FROM druid.foo where dim1 IS NOT NULL ORDER BY dim1 desc limit 100",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(BaseCalciteQueryTest.querySegmentSpec(Filtration.eternity()))
                  .columns("dim1")
                  .orderBy(ImmutableList.of(new ScanQuery.OrderBy("dim1", ScanQuery.Order.DESCENDING)))
                  .filters(new NotDimFilter(new SelectorDimFilter("dim1", null, null)))
                  .limit(100)
                  .legacy(false)
                  .context(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .build()
        ),
        expectedResults
    );
  }

  @Test
  public void testScanOrderByLimitSmallerThanDataSetSize()
  {
    //Note: set parameters -Ddruid.generic.useDefaultValueForNull=true/false
    List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{"def"}
    );

    if ("true".equals(System.getProperty("druid.generic.useDefaultValueForNull", "true"))) {
      expectedResults = ImmutableList.of(
          new Object[]{"def"}
      );
    }

    testQuery(
        "SELECT dim1 FROM druid.foo where dim1 IS NOT NULL ORDER BY dim1 desc limit 1",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(BaseCalciteQueryTest.querySegmentSpec(Filtration.eternity()))
                  .columns("dim1")
                  .orderBy(ImmutableList.of(new ScanQuery.OrderBy("dim1", ScanQuery.Order.DESCENDING)))
                  .filters(new NotDimFilter(new SelectorDimFilter("dim1", null, null)))
                  .limit(1)
                  .legacy(false)
                  .context(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .build()
        ),
        expectedResults
    );
  }

}
