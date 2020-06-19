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

package org.apache.druid.query.groupby.having;

import org.junit.Assert;
import org.junit.Test;

/**
 */
public class HavingSpecMetricComparatorTest
{
  @Test
  public void testLongRegex()
  {
    Assert.assertTrue(HavingSpecMetricComparator.LONG_PAT.matcher("1").matches());
    Assert.assertTrue(HavingSpecMetricComparator.LONG_PAT.matcher("12").matches());

    Assert.assertFalse(HavingSpecMetricComparator.LONG_PAT.matcher("1.").matches());
    Assert.assertFalse(HavingSpecMetricComparator.LONG_PAT.matcher("1.2").matches());
    Assert.assertFalse(HavingSpecMetricComparator.LONG_PAT.matcher("1.23").matches());
    Assert.assertFalse(HavingSpecMetricComparator.LONG_PAT.matcher("1E5").matches());
    Assert.assertFalse(HavingSpecMetricComparator.LONG_PAT.matcher("1.23E5").matches());

    Assert.assertFalse(HavingSpecMetricComparator.LONG_PAT.matcher("").matches());
    Assert.assertFalse(HavingSpecMetricComparator.LONG_PAT.matcher("xyz").matches());
  }

  @Test
  public void testCompareDoubleToLongWithNanReturns1()
  {
    Assert.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Double.NaN, 1));
    Assert.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Double.NaN, -1));
    Assert.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Double.NaN, Long.MAX_VALUE));
    Assert.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Double.NaN, Long.MIN_VALUE));
    Assert.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Double.NaN, 0L));

    Assert.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Float.NaN, 1));
    Assert.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Float.NaN, -1));
    Assert.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Float.NaN, Long.MAX_VALUE));
    Assert.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Float.NaN, Long.MIN_VALUE));
    Assert.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Float.NaN, 0L));
  }

  @Test
  public void testCompareDoubleToLongWithInfinityReturns1()
  {
    Assert.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Double.POSITIVE_INFINITY, 1));
    Assert.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Double.POSITIVE_INFINITY, -1));
    Assert.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Double.POSITIVE_INFINITY, Long.MAX_VALUE));
    Assert.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Double.POSITIVE_INFINITY, Long.MIN_VALUE));
    Assert.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Double.POSITIVE_INFINITY, 0L));

    Assert.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Float.POSITIVE_INFINITY, 1));
    Assert.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Float.POSITIVE_INFINITY, -1));
    Assert.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Float.POSITIVE_INFINITY, Long.MAX_VALUE));
    Assert.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Float.POSITIVE_INFINITY, Long.MIN_VALUE));
    Assert.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Float.POSITIVE_INFINITY, 0L));
  }

  @Test
  public void testCompareDoubleToLongWithInfinityReturnsNegative1()
  {
    Assert.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong(Double.NEGATIVE_INFINITY, 1));
    Assert.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong(Double.NEGATIVE_INFINITY, -1));
    Assert.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong(Double.NEGATIVE_INFINITY, Long.MAX_VALUE));
    Assert.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong(Double.NEGATIVE_INFINITY, Long.MIN_VALUE));
    Assert.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong(Double.NEGATIVE_INFINITY, 0L));

    Assert.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong(Float.NEGATIVE_INFINITY, 1));
    Assert.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong(Float.NEGATIVE_INFINITY, -1));
    Assert.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong(Float.NEGATIVE_INFINITY, Long.MAX_VALUE));
    Assert.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong(Float.NEGATIVE_INFINITY, Long.MIN_VALUE));
    Assert.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong(Float.NEGATIVE_INFINITY, 0L));
  }

  @Test
  public void testCompareDoubleToLongWithNumbers()
  {
    Assert.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(1 + 1e-6, 1));
    Assert.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong(1 - 1e-6, 1));
    Assert.assertEquals(0, HavingSpecMetricComparator.compareDoubleToLong(10D, 10));
    Assert.assertEquals(0, HavingSpecMetricComparator.compareDoubleToLong(0D, 0));
    Assert.assertEquals(0, HavingSpecMetricComparator.compareDoubleToLong(-0D, 0));
    Assert.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong((double) Long.MAX_VALUE + 1, Long.MAX_VALUE));
    Assert.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong((double) Long.MIN_VALUE - 1, Long.MIN_VALUE));
  }
}
