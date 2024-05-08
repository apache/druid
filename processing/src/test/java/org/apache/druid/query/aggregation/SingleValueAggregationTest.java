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

package org.apache.druid.query.aggregation;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.TestColumnSelectorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class SingleValueAggregationTest extends InitializedNullHandlingTest
{
  private SingleValueAggregatorFactory longAggFactory;
  private ColumnSelectorFactory colSelectorFactoryLong;
  private ColumnCapabilities columnCapabilitiesLong;
  private TestLongColumnSelector selectorLong;

  private SingleValueAggregatorFactory doubleAggFactory;
  private ColumnSelectorFactory colSelectorFactoryDouble;
  private ColumnCapabilities columnCapabilitiesDouble;
  private TestDoubleColumnSelectorImpl selectorDouble;

  private SingleValueAggregatorFactory floatAggFactory;
  private ColumnSelectorFactory colSelectorFactoryFloat;
  private ColumnCapabilities columnCapabilitiesFloat;
  private TestFloatColumnSelector selectorFloat;

  private SingleValueAggregatorFactory stringAggFactory;
  private ColumnSelectorFactory colSelectorFactoryString;
  private ColumnCapabilities columnCapabilitiesString;
  private TestObjectColumnSelector selectorString;

  private final long[] longValues = {9223372036854775802L, 9223372036854775803L};
  private final double[] doubleValues = {5.2d, 2.8976552d};
  private final float[] floatValues = {5.2f, 2.89f};
  private final String[] strValues = {"str1", "str2"};

  public SingleValueAggregationTest() throws Exception
  {
    String longAggSpecJson = "{\"type\": \"singleValue\", \"name\": \"lng\", \"fieldName\": \"lngFld\", \"columnType\": \"LONG\"}";
    longAggFactory = TestHelper.makeJsonMapper().readValue(longAggSpecJson, SingleValueAggregatorFactory.class);

    String doubleAggSpecJson = "{\"type\": \"singleValue\", \"name\": \"dbl\", \"fieldName\": \"dblFld\", \"columnType\": \"DOUBLE\"}";
    doubleAggFactory = TestHelper.makeJsonMapper().readValue(doubleAggSpecJson, SingleValueAggregatorFactory.class);

    String floatAggSpecJson = "{\"type\": \"singleValue\", \"name\": \"dbl\", \"fieldName\": \"fltFld\", \"columnType\": \"FLOAT\"}";
    floatAggFactory = TestHelper.makeJsonMapper().readValue(floatAggSpecJson, SingleValueAggregatorFactory.class);

    String strAggSpecJson = "{\"type\": \"singleValue\", \"name\": \"str\", \"fieldName\": \"strFld\", \"columnType\": \"STRING\"}";
    stringAggFactory = TestHelper.makeJsonMapper().readValue(strAggSpecJson, SingleValueAggregatorFactory.class);
  }

  @Before
  public void setup()
  {
    selectorLong = new TestLongColumnSelector(longValues);
    columnCapabilitiesLong = ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG);
    colSelectorFactoryLong = new TestColumnSelectorFactory()
        .addCapabilities("lngFld", columnCapabilitiesLong)
        .addColumnSelector("lngFld", selectorLong);

    selectorDouble = new TestDoubleColumnSelectorImpl(doubleValues);
    columnCapabilitiesDouble = ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.DOUBLE);
    colSelectorFactoryDouble = new TestColumnSelectorFactory()
        .addCapabilities("dblFld", columnCapabilitiesDouble)
        .addColumnSelector("dblFld", selectorDouble);

    selectorFloat = new TestFloatColumnSelector(floatValues);
    columnCapabilitiesFloat = ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.FLOAT);
    colSelectorFactoryFloat = new TestColumnSelectorFactory()
        .addCapabilities("fltFld", columnCapabilitiesFloat)
        .addColumnSelector("fltFld", selectorFloat);

    selectorString = new TestObjectColumnSelector(strValues);
    columnCapabilitiesString = ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities();
    colSelectorFactoryString = new TestColumnSelectorFactory()
        .addCapabilities("strFld", columnCapabilitiesString)
        .addColumnSelector("strFld", selectorString);
  }

  @Test
  public void testLongAggregator()
  {
    Assert.assertEquals(ColumnType.LONG, longAggFactory.getIntermediateType());
    Assert.assertEquals(ColumnType.LONG, longAggFactory.getResultType());
    Assert.assertEquals("lng", longAggFactory.getName());
    Assert.assertEquals("lngFld", longAggFactory.getFieldName());
    Assert.assertThrows(DruidException.class, () -> longAggFactory.getComparator());

    Aggregator agg = longAggFactory.factorize(colSelectorFactoryLong);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertFalse(agg.isNull());
      Assert.assertEquals(0L, agg.getLong());
    } else {
      Assert.assertTrue(agg.isNull());
      Assert.assertThrows(AssertionError.class, () -> agg.getLong());
    }

    aggregate(selectorLong, agg);
    Assert.assertEquals(longValues[0], ((Long) agg.get()).longValue());
    Assert.assertEquals(longValues[0], agg.getLong());

    Assert.assertThrows(DruidException.class, () -> aggregate(selectorLong, agg));
  }

  @Test
  public void testLongBufferAggregator()
  {
    BufferAggregator agg = longAggFactory.factorizeBuffered(colSelectorFactoryLong);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[Double.BYTES + Byte.BYTES]);
    agg.init(buffer, 0);
    Assert.assertEquals(0L, agg.getLong(buffer, 0));

    aggregate(selectorLong, agg, buffer, 0);
    Assert.assertEquals(longValues[0], ((Long) agg.get(buffer, 0)).longValue());
    Assert.assertEquals(longValues[0], agg.getLong(buffer, 0));

    Assert.assertThrows(DruidException.class, () -> aggregate(selectorLong, agg, buffer, 0));
  }

  @Test
  public void testCombine()
  {
    Assert.assertThrows(DruidException.class, () -> longAggFactory.combine(9223372036854775800L, 9223372036854775803L));
  }

  @Test
  public void testDoubleAggregator()
  {
    Aggregator agg = doubleAggFactory.factorize(colSelectorFactoryDouble);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(0.0d, agg.getDouble(), 0.000001);
    } else {
      Assert.assertThrows(AssertionError.class, () -> agg.getDouble());
    }

    aggregate(selectorDouble, agg);
    Assert.assertEquals(doubleValues[0], ((Double) agg.get()).doubleValue(), 0.000001);
    Assert.assertEquals(doubleValues[0], agg.getDouble(), 0.000001);

    Assert.assertThrows(DruidException.class, () -> aggregate(selectorDouble, agg));
  }

  @Test
  public void testDoubleBufferAggregator()
  {
    BufferAggregator agg = doubleAggFactory.factorizeBuffered(colSelectorFactoryDouble);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[SingleValueAggregatorFactory.DEFAULT_MAX_VALUE_SIZE + Byte.BYTES]);
    agg.init(buffer, 0);
    Assert.assertEquals(0.0d, agg.getDouble(buffer, 0), 0.000001);

    aggregate(selectorDouble, agg, buffer, 0);
    Assert.assertEquals(doubleValues[0], ((Double) agg.get(buffer, 0)).doubleValue(), 0.000001);
    Assert.assertEquals(doubleValues[0], agg.getDouble(buffer, 0), 0.000001);

    Assert.assertThrows(DruidException.class, () -> aggregate(selectorDouble, agg, buffer, 0));
  }

  @Test
  public void testFloatAggregator()
  {
    Aggregator agg = floatAggFactory.factorize(colSelectorFactoryFloat);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(0.0f, agg.getFloat(), 0.000001);
    } else {
      Assert.assertThrows(AssertionError.class, () -> agg.getFloat());
    }

    aggregate(selectorFloat, agg);
    Assert.assertEquals(floatValues[0], ((Float) agg.get()).floatValue(), 0.000001);
    Assert.assertEquals(floatValues[0], agg.getFloat(), 0.000001);

    Assert.assertThrows(DruidException.class, () -> aggregate(selectorFloat, agg));
  }

  @Test
  public void testFloatBufferAggregator()
  {
    BufferAggregator agg = floatAggFactory.factorizeBuffered(colSelectorFactoryFloat);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[Double.BYTES + Byte.BYTES]);
    agg.init(buffer, 0);
    Assert.assertEquals(0.0f, agg.getFloat(buffer, 0), 0.000001);

    aggregate(selectorFloat, agg, buffer, 0);
    Assert.assertEquals(floatValues[0], ((Float) agg.get(buffer, 0)).floatValue(), 0.000001);
    Assert.assertEquals(floatValues[0], agg.getFloat(buffer, 0), 0.000001);

    Assert.assertThrows(DruidException.class, () -> aggregate(selectorFloat, agg, buffer, 0));
  }

  @Test
  public void testStringAggregator()
  {
    Aggregator agg = stringAggFactory.factorize(colSelectorFactoryString);

    Assert.assertEquals(null, agg.get());

    aggregate(selectorString, agg);
    Assert.assertEquals(strValues[0], agg.get());

    Assert.assertThrows(DruidException.class, () -> aggregate(selectorString, agg));
  }

  @Test
  public void testStringBufferAggregator()
  {
    BufferAggregator agg = stringAggFactory.factorizeBuffered(colSelectorFactoryString);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[SingleValueAggregatorFactory.DEFAULT_MAX_VALUE_SIZE + Byte.BYTES]);
    agg.init(buffer, 0);

    aggregate(selectorString, agg, buffer, 0);
    Assert.assertEquals(strValues[0], agg.get(buffer, 0));

    Assert.assertThrows(DruidException.class, () -> aggregate(selectorString, agg, buffer, 0));
  }

  @Test
  public void testEqualsAndHashCode()
  {
    SingleValueAggregatorFactory one = new SingleValueAggregatorFactory("name1", "fieldName1", ColumnType.LONG);
    SingleValueAggregatorFactory oneMore = new SingleValueAggregatorFactory("name1", "fieldName1", ColumnType.LONG);
    SingleValueAggregatorFactory two = new SingleValueAggregatorFactory("name2", "fieldName2", ColumnType.LONG);

    Assert.assertEquals(one.hashCode(), oneMore.hashCode());

    Assert.assertTrue(one.equals(oneMore));
    Assert.assertFalse(one.equals(two));
  }

  private void aggregate(TestLongColumnSelector selector, Aggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  private void aggregate(TestLongColumnSelector selector, BufferAggregator agg, ByteBuffer buff, int position)
  {
    agg.aggregate(buff, position);
    selector.increment();
  }

  private void aggregate(TestFloatColumnSelector selector, Aggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  private void aggregate(TestFloatColumnSelector selector, BufferAggregator agg, ByteBuffer buff, int position)
  {
    agg.aggregate(buff, position);
    selector.increment();
  }

  private void aggregate(TestDoubleColumnSelectorImpl selector, Aggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  private void aggregate(TestDoubleColumnSelectorImpl selector, BufferAggregator agg, ByteBuffer buff, int position)
  {
    agg.aggregate(buff, position);
    selector.increment();
  }

  private void aggregate(TestObjectColumnSelector selector, Aggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  private void aggregate(TestObjectColumnSelector selector, BufferAggregator agg, ByteBuffer buff, int position)
  {
    agg.aggregate(buff, position);
    selector.increment();
  }
}
