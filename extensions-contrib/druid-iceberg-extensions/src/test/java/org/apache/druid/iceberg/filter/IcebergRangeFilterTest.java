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

package org.apache.druid.iceberg.filter;

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.junit.Assert;
import org.junit.Test;

public class IcebergRangeFilterTest
{
  private final String TEST_COLUMN = "column1";

  @Test
  public void testUpperOpenFilter()
  {
    Expression expectedExpression = Expressions.and(
        Expressions.greaterThanOrEqual(TEST_COLUMN, 45),
        Expressions.lessThan(TEST_COLUMN, 50)
    );
    IcebergRangeFilter rangeFilter = new IcebergRangeFilter(TEST_COLUMN, 45, 50, false, true);
    Assert.assertEquals(expectedExpression.toString(), rangeFilter.getFilterExpression().toString());
  }

  @Test
  public void testLowerOpenFilter()
  {
    Expression expectedExpression = Expressions.and(
        Expressions.greaterThan(TEST_COLUMN, 45),
        Expressions.lessThanOrEqual(TEST_COLUMN, 50)
    );
    IcebergRangeFilter rangeFilter = new IcebergRangeFilter(TEST_COLUMN, 45, 50, true, false);
    Assert.assertEquals(expectedExpression.toString(), rangeFilter.getFilterExpression().toString());
  }

  @Test
  public void testNoLowerFilter()
  {
    Expression expectedExpression = Expressions.lessThanOrEqual(TEST_COLUMN, 50);
    IcebergRangeFilter rangeFilter = new IcebergRangeFilter(TEST_COLUMN, null, 50, null, false);
    Assert.assertEquals(expectedExpression.toString(), rangeFilter.getFilterExpression().toString());
  }

  @Test
  public void testNoUpperFilter()
  {
    Expression expectedExpression = Expressions.greaterThanOrEqual(TEST_COLUMN, 100);
    IcebergRangeFilter rangeFilter = new IcebergRangeFilter(TEST_COLUMN, 100, null, null, null);
    Assert.assertEquals(expectedExpression.toString(), rangeFilter.getFilterExpression().toString());
  }
}
