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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class ColumnComparisonDimFilterTest
{
  @Test
  public void testGetCacheKey()
  {
    ColumnComparisonDimFilter columnComparisonDimFilter = new ColumnComparisonDimFilter(ImmutableList.of(
        DefaultDimensionSpec.of("abc"),
        DefaultDimensionSpec.of("d")
    ));
    ColumnComparisonDimFilter columnComparisonDimFilter2 = new ColumnComparisonDimFilter(ImmutableList.of(
        DefaultDimensionSpec.of("d"),
        DefaultDimensionSpec.of("abc")
    ));
    ColumnComparisonDimFilter columnComparisonDimFilter3 = new ColumnComparisonDimFilter(
        ImmutableList.of(
            DefaultDimensionSpec.of("d"),
            DefaultDimensionSpec.of("e")
        )
    );

    Assert.assertTrue(Arrays.equals(
        columnComparisonDimFilter.getCacheKey(),
        columnComparisonDimFilter2.getCacheKey()
    ));
    Assert.assertFalse(Arrays.equals(
        columnComparisonDimFilter2.getCacheKey(),
        columnComparisonDimFilter3.getCacheKey()
    ));
  }

  @Test
  public void testHashCode()
  {
    ColumnComparisonDimFilter columnComparisonDimFilter = new ColumnComparisonDimFilter(
        ImmutableList.of(
            DefaultDimensionSpec.of("abc"),
            DefaultDimensionSpec.of("d")
        )
    );
    ColumnComparisonDimFilter columnComparisonDimFilter2 = new ColumnComparisonDimFilter(
        ImmutableList.of(
            DefaultDimensionSpec.of("d"),
            DefaultDimensionSpec.of("abc")
        )
    );
    ColumnComparisonDimFilter columnComparisonDimFilter3 = new ColumnComparisonDimFilter(
        ImmutableList.of(
            DefaultDimensionSpec.of("d"),
            DefaultDimensionSpec.of("e")
        )
    );

    Assert.assertNotEquals(
        columnComparisonDimFilter.hashCode(),
        columnComparisonDimFilter2.hashCode()
    );
    Assert.assertNotEquals(
        columnComparisonDimFilter2.hashCode(),
        columnComparisonDimFilter3.hashCode()
    );
  }

  @Test
  public void testGetRequiredColumns()
  {
    ColumnComparisonDimFilter columnComparisonDimFilter = new ColumnComparisonDimFilter(
        ImmutableList.of(
            DefaultDimensionSpec.of("abc"),
            DefaultDimensionSpec.of("d")
        )
    );
    Assert.assertEquals(columnComparisonDimFilter.getRequiredColumns(), Sets.newHashSet("abc", "d"));
  }
}
