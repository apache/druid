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

import org.apache.druid.segment.filter.FilterTestUtils;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

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
    Assert.assertEquals(expected, dimFilter.toFilter());
  }

  @Test
  public void testOptimieShortCircuitWithTrueFilter()
  {
    DimFilter filter = DimFilterTestUtils.or(
        DimFilterTestUtils.selector("col1", "1"),
        TrueDimFilter.instance()
    );
    Assert.assertTrue(filter.optimize() instanceof TrueDimFilter);
  }

  @Test
  public void testOptimizeOringFalseFilters()
  {
    DimFilter filter = DimFilterTestUtils.or(FalseDimFilter.instance(), FalseDimFilter.instance());
    Assert.assertSame(FalseDimFilter.instance(), filter.optimize());
  }

  @Test
  public void testOptimizeOrOfSingleFilterUnwrapOr()
  {
    DimFilter expected = DimFilterTestUtils.selector("col1", "1");
    DimFilter filter = DimFilterTestUtils.or(expected);
    Assert.assertEquals(expected, filter.optimize());
  }

  @Test
  public void testOptimizeOrOfMultipleFiltersReturningAsItIs()
  {
    DimFilter filter = DimFilterTestUtils.or(
        DimFilterTestUtils.selector("col1", "1"),
        DimFilterTestUtils.selector("col1", "2")
    );
    Assert.assertEquals(filter, filter.optimize());
  }
}
