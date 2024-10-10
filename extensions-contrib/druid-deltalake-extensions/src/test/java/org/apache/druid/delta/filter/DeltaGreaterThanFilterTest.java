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

package org.apache.druid.delta.filter;

import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import org.junit.Assert;
import org.junit.Test;

public class DeltaGreaterThanFilterTest
{
  private static final StructType SCHEMA = new StructType()
      .add(new StructField("str_col", StringType.STRING, true))
      .add(new StructField("int_col", IntegerType.INTEGER, true))
      .add(new StructField("short_col", ShortType.SHORT, true))
      .add(new StructField("long_col", LongType.LONG, false))
      .add(new StructField("float_col", FloatType.FLOAT, true))
      .add(new StructField("double_col", DoubleType.DOUBLE, true))
      .add(new StructField("date_col", DateType.DATE, true));

  @Test
  public void testGreaterThanFilter()
  {
    DeltaGreaterThanFilter gtFilter = new DeltaGreaterThanFilter("int_col", "123");

    Predicate predicate = gtFilter.getFilterPredicate(SCHEMA);

    Assert.assertEquals(">", predicate.getName());
    Assert.assertEquals(2, predicate.getChildren().size());
  }
}
