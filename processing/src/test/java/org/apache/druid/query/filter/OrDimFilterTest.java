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

package org.apache.druid.query.filter;

import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.filter.FalseFilter;
import org.apache.druid.segment.filter.FilterTestUtils;
import org.apache.druid.segment.filter.TrueFilter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.partition.TypedValueSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;

public class OrDimFilterTest extends InitializedNullHandlingTest
{
  @Test
  public void testToFilterWithDuplicateFilters()
  {
    DimFilter dimFilter = DimFilterTestUtils.or(
        DimFilterTestUtils.and(
            DimFilterTestUtils.selector("col1", "1"),
            DimFilterTestUtils.selector("col2", "2")
        ),
        DimFilterTestUtils.and(
            // duplicate but different order
            DimFilterTestUtils.selector("col2", "2"),
            DimFilterTestUtils.selector("col1", "1")
        ),
        DimFilterTestUtils.selector("col3", "3")
    );
    Filter expected = FilterTestUtils.or(
        FilterTestUtils.and(
            FilterTestUtils.selector("col1", "1"),
            FilterTestUtils.selector("col2", "2")
        ),
        FilterTestUtils.selector("col3", "3")
    );
    Assertions.assertEquals(expected, dimFilter.toFilter());
  }

  @Test
  public void testToFilterShortCircuitWithTrueFilter()
  {
    DimFilter filter = DimFilterTestUtils.or(
        DimFilterTestUtils.selector("col1", "1"),
        TrueDimFilter.instance()
    );
    Assertions.assertTrue(filter.toFilter() instanceof TrueFilter);
  }

  @Test
  public void testToFilterOringFalseFilters()
  {
    DimFilter filter = DimFilterTestUtils.or(FalseDimFilter.instance(), FalseDimFilter.instance());
    Assertions.assertSame(FalseFilter.instance(), filter.toFilter());
  }

  @Test
  public void testOptimizeOrOfSingleFilterUnwrapOr()
  {
    DimFilter expected = DimFilterTestUtils.selector("col1", "1");
    DimFilter filter = DimFilterTestUtils.or(expected);
    Assertions.assertEquals(expected.toFilter(), filter.toFilter());
  }

  @Test
  public void testOptimizeOrOfMultipleFiltersReturningAsItIs()
  {
    DimFilter filter = DimFilterTestUtils.or(
        DimFilterTestUtils.selector("col1", "1"),
        DimFilterTestUtils.selector("col1", "2")
    );
    Assertions.assertEquals(filter.toFilter(), filter.toFilter());
  }

  @Test
  public void testGetDimensionValueSet_unionsBranchesOnSameDim()
  {
    // code = 5 OR code = 6 → union {5,6}.
    final DimFilter filter = new OrDimFilter(
        new EqualityFilter("code", ColumnType.LONG, 5L, null),
        new EqualityFilter("code", ColumnType.LONG, 6L, null)
    );
    final TypedValueSet valueSet = filter.getDimensionValueSet("code");
    Assertions.assertNotNull(valueSet);
    Assertions.assertEquals(ColumnType.LONG, valueSet.getType());
    Assertions.assertEquals(new HashSet<>(Arrays.asList("5", "6")), valueSet.getValues());
  }

  @Test
  public void testGetDimensionValueSet_anyUnconstrainedBranch_returnsNull()
  {
    // code = 5 OR region = 'us': the region branch matches any code, so code cannot be pruned.
    final DimFilter filter = new OrDimFilter(
        new EqualityFilter("code", ColumnType.LONG, 5L, null),
        new SelectorDimFilter("region", "us", null)
    );
    Assertions.assertNull(filter.getDimensionValueSet("code"));
  }

  @Test
  public void testGetDimensionValueSet_branchOnDifferentDim_returnsNull()
  {
    // Querying a dimension the OR does not constrain → null.
    final DimFilter filter = new OrDimFilter(
        new EqualityFilter("code", ColumnType.LONG, 5L, null),
        new EqualityFilter("code", ColumnType.LONG, 6L, null)
    );
    Assertions.assertNull(filter.getDimensionValueSet("region"));
  }
}
