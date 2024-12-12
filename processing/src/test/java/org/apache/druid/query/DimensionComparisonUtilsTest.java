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

package org.apache.druid.query;

import com.google.common.primitives.Ints;
import org.apache.druid.query.ordering.StringComparator;
import org.junit.Assert;
import org.junit.Test;

public class DimensionComparisonUtilsTest
{
  @Test
  public void testArrayComparator()
  {
    // Element comparator returns the inverse of what the comparator should return
    DimensionComparisonUtils.ArrayComparator<Long> longArrayComparator = new DimensionComparisonUtils.ArrayComparator<>(
        (a, b) -> {
          if (a == null) {
            return b == null ? 0 : 1;
          }
          return b == null ? -1 : -a.compareTo(b);
        }
    );
    Assert.assertEquals(1, longArrayComparator.compare(new Object[]{1L, 2L}, new Object[]{5L, 6L}));
    Assert.assertEquals(-1, longArrayComparator.compare(null, new Object[]{1L}));
    Assert.assertEquals(1, longArrayComparator.compare(new Object[]{1L}, null));
    Assert.assertEquals(1, longArrayComparator.compare(new Object[]{null}, new Object[]{1L}));
    //noinspection EqualsWithItself
    Assert.assertEquals(0, longArrayComparator.compare(null, null));
    //noinspection EqualsWithItself
    Assert.assertEquals(0, longArrayComparator.compare(new Object[]{1L, 2L, null, 3L}, new Object[]{1L, 2L, null, 3L}));
    Assert.assertEquals(1, longArrayComparator.compare(new Object[]{null}, new Object[]{1L}));
  }

  @Test
  public void testNumericArrayComparator()
  {
    // Element comparator compares the length of the string representation of the object
    DimensionComparisonUtils.ArrayComparatorForUnnaturalStringComparator arrayComparator =
        new DimensionComparisonUtils.ArrayComparatorForUnnaturalStringComparator(new StringComparator()
        {
          @Override
          public byte[] getCacheKey()
          {
            return new byte[]{(byte) 0xFF};
          }

          @Override
          public int compare(String s, String t1)
          {
            return Ints.compare(s.length(), t1.length());
          }
        });
    //noinspection EqualsWithItself
    Assert.assertEquals(0, arrayComparator.compare(null, null));
    //noinspection EqualsWithItself
    Assert.assertEquals(0, arrayComparator.compare(new Object[]{1L, 2L, null, 3L}, new Object[]{1L, 2L, null, 3L}));
    // All elements have same length when converted to string
    Assert.assertEquals(0, arrayComparator.compare(new Object[]{1L, 2L}, new Object[]{5L, 6L}));
    Assert.assertEquals(-1, arrayComparator.compare(null, new Object[]{1L}));
    Assert.assertEquals(1, arrayComparator.compare(new Object[]{1L}, null));
    // "null" > "1"
    Assert.assertEquals(1, arrayComparator.compare(new Object[]{null}, new Object[]{1L}));
    Assert.assertEquals(-1, arrayComparator.compare(new Object[]{1L}, new Object[]{1.1f}));
  }
}
