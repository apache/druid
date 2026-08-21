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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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

public class AndDimFilterTest extends InitializedNullHandlingTest
{
  @Test
  public void testGetRequiredColumns()
  {
    AndDimFilter andDimFilter = new AndDimFilter(
        Lists.newArrayList(
            new SelectorDimFilter("a", "d", null),
            new SelectorDimFilter("b", "d", null),
            new SelectorDimFilter("c", "d", null)
        )
    );
    Assertions.assertEquals(andDimFilter.getRequiredColumns(), Sets.newHashSet("a", "b", "c"));
  }

  @Test
  public void testToFilterWithDuplicateFilters()
  {
    DimFilter dimFilter = DimFilterTestUtils.and(
        DimFilterTestUtils.or(
            DimFilterTestUtils.selector("col1", "1"),
            DimFilterTestUtils.selector("col2", "2")
        ),
        DimFilterTestUtils.or(
            // duplicate but different order
            DimFilterTestUtils.selector("col2", "2"),
            DimFilterTestUtils.selector("col1", "1")
        ),
        DimFilterTestUtils.selector("col3", "3")
    );
    Filter expected = FilterTestUtils.and(
        FilterTestUtils.or(
            FilterTestUtils.selector("col1", "1"),
            FilterTestUtils.selector("col2", "2")
        ),
        FilterTestUtils.selector("col3", "3")
    );
    Assertions.assertEquals(expected, dimFilter.toFilter());
  }

  @Test
  public void testToFilterShortCircuitWithFalseFilter()
  {
    DimFilter filter = DimFilterTestUtils.and(
        DimFilterTestUtils.selector("col1", "1"),
        FalseDimFilter.instance()
    );
    Assertions.assertTrue(filter.toFilter() instanceof FalseFilter);
  }

  @Test
  public void testToFilterOringTrueFilters()
  {
    DimFilter filter = DimFilterTestUtils.and(TrueDimFilter.instance(), TrueDimFilter.instance());
    Assertions.assertSame(TrueFilter.instance(), filter.toFilter());
  }

  @Test
  public void testToFilterAndOfSingleFilterUnwrapAnd()
  {
    DimFilter expected = DimFilterTestUtils.selector("col1", "1");
    DimFilter filter = DimFilterTestUtils.and(expected);
    Assertions.assertEquals(expected.toFilter(), filter.toFilter());
  }

  @Test
  public void testToFilterAndOfMultipleFiltersReturningAsItIs()
  {
    DimFilter filter = DimFilterTestUtils.and(
        DimFilterTestUtils.selector("col1", "1"),
        DimFilterTestUtils.selector("col1", "2")
    );
    Assertions.assertEquals(filter.toFilter(), filter.toFilter());
  }

  @Test
  public void testGetDimensionValueSet_singleConstrainingBranch()
  {
    // AND with one constraining branch → value set is that branch.
    final DimFilter filter = new AndDimFilter(
        new EqualityFilter("code", ColumnType.LONG, 5L, null),
        new SelectorDimFilter("region", "us", null)
    );
    final TypedValueSet valueSet = filter.getDimensionValueSet("code");
    Assertions.assertNotNull(valueSet);
    Assertions.assertEquals(ColumnType.LONG, valueSet.getType());
    Assertions.assertEquals(new HashSet<>(java.util.List.of("5")), valueSet.getValues());
    // A dimension no branch constrains → null.
    Assertions.assertNull(filter.getDimensionValueSet("region"));
  }

  @Test
  public void testGetDimensionValueSet_intersectsBranchesOnSameDim()
  {
    // code = 5 AND code IN (5,6) → intersection {5}.
    final DimFilter filter = new AndDimFilter(
        new EqualityFilter("code", ColumnType.LONG, 5L, null),
        new TypedInFilter("code", ColumnType.LONG, Arrays.asList(5L, 6L), null, null)
    );
    final TypedValueSet valueSet = filter.getDimensionValueSet("code");
    Assertions.assertNotNull(valueSet);
    Assertions.assertEquals(new HashSet<>(java.util.List.of("5")), valueSet.getValues());
  }

  @Test
  public void testGetDimensionValueSet_disjointBranches_emptyIntersection()
  {
    // code = 5 AND code = 6 → empty intersection, which prunes everything.
    final DimFilter filter = new AndDimFilter(
        new EqualityFilter("code", ColumnType.LONG, 5L, null),
        new EqualityFilter("code", ColumnType.LONG, 6L, null)
    );
    final TypedValueSet valueSet = filter.getDimensionValueSet("code");
    Assertions.assertNotNull(valueSet);
    Assertions.assertTrue(valueSet.getValues().isEmpty());
  }

  @Test
  public void testGetDimensionValueSet_noConstrainingBranch_returnsNull()
  {
    final DimFilter filter = new AndDimFilter(
        new SelectorDimFilter("region", "us", null),
        new SelectorDimFilter("tenant", "a", null)
    );
    Assertions.assertNull(filter.getDimensionValueSet("code"));
  }
}
