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

import org.apache.druid.error.DruidException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

/**
 *
 */
public class HavingSpecMetricComparatorTest
{
  @Test
  public void testLongRegex()
  {
    Assertions.assertTrue(HavingSpecMetricComparator.LONG_PAT.matcher("1").matches());
    Assertions.assertTrue(HavingSpecMetricComparator.LONG_PAT.matcher("12").matches());

    Assertions.assertFalse(HavingSpecMetricComparator.LONG_PAT.matcher("1.").matches());
    Assertions.assertFalse(HavingSpecMetricComparator.LONG_PAT.matcher("1.2").matches());
    Assertions.assertFalse(HavingSpecMetricComparator.LONG_PAT.matcher("1.23").matches());
    Assertions.assertFalse(HavingSpecMetricComparator.LONG_PAT.matcher("1E5").matches());
    Assertions.assertFalse(HavingSpecMetricComparator.LONG_PAT.matcher("1.23E5").matches());

    Assertions.assertFalse(HavingSpecMetricComparator.LONG_PAT.matcher("").matches());
    Assertions.assertFalse(HavingSpecMetricComparator.LONG_PAT.matcher("xyz").matches());
  }

  @Test
  public void testCompareDoubleToLongWithNanReturns1()
  {
    Assertions.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Double.NaN, 1));
    Assertions.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Double.NaN, -1));
    Assertions.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Double.NaN, Long.MAX_VALUE));
    Assertions.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Double.NaN, Long.MIN_VALUE));
    Assertions.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Double.NaN, 0L));

    Assertions.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Float.NaN, 1));
    Assertions.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Float.NaN, -1));
    Assertions.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Float.NaN, Long.MAX_VALUE));
    Assertions.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Float.NaN, Long.MIN_VALUE));
    Assertions.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Float.NaN, 0L));
  }

  @Test
  public void testCompareDoubleToLongWithInfinityReturns1()
  {
    Assertions.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Double.POSITIVE_INFINITY, 1));
    Assertions.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Double.POSITIVE_INFINITY, -1));
    Assertions.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Double.POSITIVE_INFINITY, Long.MAX_VALUE));
    Assertions.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Double.POSITIVE_INFINITY, Long.MIN_VALUE));
    Assertions.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Double.POSITIVE_INFINITY, 0L));

    Assertions.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Float.POSITIVE_INFINITY, 1));
    Assertions.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Float.POSITIVE_INFINITY, -1));
    Assertions.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Float.POSITIVE_INFINITY, Long.MAX_VALUE));
    Assertions.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Float.POSITIVE_INFINITY, Long.MIN_VALUE));
    Assertions.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(Float.POSITIVE_INFINITY, 0L));
  }

  @Test
  public void testCompareDoubleToLongWithInfinityReturnsNegative1()
  {
    Assertions.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong(Double.NEGATIVE_INFINITY, 1));
    Assertions.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong(Double.NEGATIVE_INFINITY, -1));
    Assertions.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong(Double.NEGATIVE_INFINITY, Long.MAX_VALUE));
    Assertions.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong(Double.NEGATIVE_INFINITY, Long.MIN_VALUE));
    Assertions.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong(Double.NEGATIVE_INFINITY, 0L));

    Assertions.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong(Float.NEGATIVE_INFINITY, 1));
    Assertions.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong(Float.NEGATIVE_INFINITY, -1));
    Assertions.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong(Float.NEGATIVE_INFINITY, Long.MAX_VALUE));
    Assertions.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong(Float.NEGATIVE_INFINITY, Long.MIN_VALUE));
    Assertions.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong(Float.NEGATIVE_INFINITY, 0L));
  }

  @Test
  public void testCompareDoubleToLongWithNumbers()
  {
    Assertions.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong(1 + 1e-6, 1));
    Assertions.assertEquals(-1, HavingSpecMetricComparator.compareDoubleToLong(1 - 1e-6, 1));
    Assertions.assertEquals(0, HavingSpecMetricComparator.compareDoubleToLong(10D, 10));
    Assertions.assertEquals(0, HavingSpecMetricComparator.compareDoubleToLong(0D, 0));
    Assertions.assertEquals(0, HavingSpecMetricComparator.compareDoubleToLong(-0D, 0));
    Assertions.assertEquals(1, HavingSpecMetricComparator.compareDoubleToLong((double) Long.MAX_VALUE + 1, Long.MAX_VALUE));
    Assertions.assertEquals(
        -1,
        HavingSpecMetricComparator.compareDoubleToLong((double) Long.MIN_VALUE - 1, Long.MIN_VALUE)
    );
  }

  @Test
  public void testNullValue()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> HavingSpecMetricComparator.compare("metric", null, null, 10)
    );

    Assertions.assertEquals(DruidException.Category.DEFENSIVE, e.getCategory());
  }

  @Test
  public void testNullMetricValue()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> HavingSpecMetricComparator.compare("metric", 10, null, null)
    );

    Assertions.assertEquals(DruidException.Category.DEFENSIVE, e.getCategory());
  }

  @Test
  public void testUnsupportedNumberTypeLongValue()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> HavingSpecMetricComparator.compare("metric", BigDecimal.valueOf(10), null, 10)
    );

    Assertions.assertEquals(DruidException.Category.DEFENSIVE, e.getCategory());
  }

  @Test
  public void testUnsupportedNumberTypeDoubleValue()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> HavingSpecMetricComparator.compare("metric", BigDecimal.valueOf(10), null, 10d)
    );

    Assertions.assertEquals(DruidException.Category.DEFENSIVE, e.getCategory());
  }
}
