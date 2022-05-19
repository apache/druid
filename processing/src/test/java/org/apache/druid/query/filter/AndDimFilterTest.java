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
import org.apache.druid.segment.filter.FalseFilter;
import org.apache.druid.segment.filter.FilterTestUtils;
import org.apache.druid.segment.filter.TrueFilter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

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
    Assert.assertEquals(andDimFilter.getRequiredColumns(), Sets.newHashSet("a", "b", "c"));
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
    Assert.assertEquals(expected, dimFilter.toFilter());
  }

  @Test
  public void testToFilterShortCircuitWithFalseFilter()
  {
    DimFilter filter = DimFilterTestUtils.and(
        DimFilterTestUtils.selector("col1", "1"),
        FalseDimFilter.instance()
    );
    Assert.assertTrue(filter.toFilter() instanceof FalseFilter);
  }

  @Test
  public void testToFilterOringTrueFilters()
  {
    DimFilter filter = DimFilterTestUtils.and(TrueDimFilter.instance(), TrueDimFilter.instance());
    Assert.assertSame(TrueFilter.instance(), filter.toFilter());
  }

  @Test
  public void testToFilterAndOfSingleFilterUnwrapAnd()
  {
    DimFilter expected = DimFilterTestUtils.selector("col1", "1");
    DimFilter filter = DimFilterTestUtils.and(expected);
    Assert.assertEquals(expected.toFilter(), filter.toFilter());
  }

  @Test
  public void testToFilterAndOfMultipleFiltersReturningAsItIs()
  {
    DimFilter filter = DimFilterTestUtils.and(
        DimFilterTestUtils.selector("col1", "1"),
        DimFilterTestUtils.selector("col1", "2")
    );
    Assert.assertEquals(filter.toFilter(), filter.toFilter());
  }
}
