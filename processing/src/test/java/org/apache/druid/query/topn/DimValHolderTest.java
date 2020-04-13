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

package org.apache.druid.query.topn;

import org.apache.druid.segment.column.ValueType;
import org.junit.Assert;
import org.junit.Test;

public class DimValHolderTest
{
  @Test
  public void testDimTypeConversion()
  {
    DimValHolder.Builder builder = new DimValHolder.Builder();

    builder.withDimValue("1", ValueType.STRING);
    Assert.assertEquals("1", builder.build().getDimValue());
    builder.withDimValue("1", ValueType.LONG);
    Assert.assertEquals(1L, builder.build().getDimValue());
    builder.withDimValue("1", ValueType.FLOAT);
    Assert.assertEquals(1f, builder.build().getDimValue());
    builder.withDimValue("1", ValueType.DOUBLE);
    Assert.assertEquals(1d, builder.build().getDimValue());

    builder.withDimValue(1L, ValueType.STRING);
    Assert.assertEquals("1", builder.build().getDimValue());
    builder.withDimValue(1L, ValueType.LONG);
    Assert.assertEquals(1L, builder.build().getDimValue());
    builder.withDimValue(1L, ValueType.FLOAT);
    Assert.assertEquals(1f, builder.build().getDimValue());
    builder.withDimValue(1L, ValueType.DOUBLE);
    Assert.assertEquals(1d, builder.build().getDimValue());

    builder.withDimValue(1f, ValueType.STRING);
    Assert.assertEquals("1.0", builder.build().getDimValue());
    builder.withDimValue(1f, ValueType.LONG);
    Assert.assertEquals(1L, builder.build().getDimValue());
    builder.withDimValue(1f, ValueType.FLOAT);
    Assert.assertEquals(1f, builder.build().getDimValue());
    builder.withDimValue(1f, ValueType.DOUBLE);
    Assert.assertEquals(1d, builder.build().getDimValue());

    builder.withDimValue(1d, ValueType.STRING);
    Assert.assertEquals("1.0", builder.build().getDimValue());
    builder.withDimValue(1d, ValueType.LONG);
    Assert.assertEquals(1L, builder.build().getDimValue());
    builder.withDimValue(1d, ValueType.FLOAT);
    Assert.assertEquals(1f, builder.build().getDimValue());
    builder.withDimValue(1d, ValueType.DOUBLE);
    Assert.assertEquals(1d, builder.build().getDimValue());
  }
}
