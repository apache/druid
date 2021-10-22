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

package org.apache.druid.query.aggregation.any;

import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.TestFloatColumnSelector;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class FloatAnyAggregationTest extends InitializedNullHandlingTest
{
  private FloatAnyAggregatorFactory floatAnyAggFactory;
  private FloatAnyAggregatorFactory combiningAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestFloatColumnSelector valueSelector;
  private TestObjectColumnSelector objectSelector;

  private float[] floats = {1.1897f, 0.001f, 86.23f, 166.228f};
  private Float[] objects = {2.1897f, 1.001f, 87.23f, 167.228f};

  @Before
  public void setup()
  {
    floatAnyAggFactory = new FloatAnyAggregatorFactory("billy", "nilly");
    combiningAggFactory = (FloatAnyAggregatorFactory) floatAnyAggFactory.getCombiningFactory();
    valueSelector = new TestFloatColumnSelector(floats);
    objectSelector = new TestObjectColumnSelector<>(objects);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(valueSelector);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("billy")).andReturn(objectSelector);
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testFloatAnyAggregator()
  {
    Aggregator agg = floatAnyAggFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Float result = (Float) agg.get();

    Assert.assertEquals((Float) floats[0], result);
    Assert.assertEquals((long) floats[0], agg.getLong());
    Assert.assertEquals(floats[0], agg.getFloat(), 0.0001);
  }

  @Test
  public void testFloatAnyBufferAggregator()
  {
    BufferAggregator agg = floatAnyAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[floatAnyAggFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Float result = (Float) agg.get(buffer, 0);

    Assert.assertEquals(floats[0], result, 0.0001);
    Assert.assertEquals((long) floats[0], agg.getLong(buffer, 0));
    Assert.assertEquals(floats[0], agg.getFloat(buffer, 0), 0.0001);
  }

  @Test
  public void testCombine()
  {
    Float f1 = 3.0f;
    Float f2 = 4.0f;
    Assert.assertEquals(f1, floatAnyAggFactory.combine(f1, f2));
  }

  @Test
  public void testComparatorWithNulls()
  {
    Float f1 = 3.0f;
    Float f2 = null;
    Comparator comparator = floatAnyAggFactory.getComparator();
    Assert.assertEquals(1, comparator.compare(f1, f2));
    Assert.assertEquals(0, comparator.compare(f1, f1));
    Assert.assertEquals(0, comparator.compare(f2, f2));
    Assert.assertEquals(-1, comparator.compare(f2, f1));
  }

  @Test
  public void testFloatAnyCombiningAggregator()
  {
    Aggregator agg = combiningAggFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Float result = (Float) agg.get();

    Assert.assertEquals(objects[0], result, 0.0001);
    Assert.assertEquals(objects[0].longValue(), agg.getLong());
    Assert.assertEquals(objects[0], agg.getFloat(), 0.0001);
  }

  @Test
  public void testFloatAnyCombiningBufferAggregator()
  {
    BufferAggregator agg = combiningAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[floatAnyAggFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Float result = (Float) agg.get(buffer, 0);

    Assert.assertEquals(objects[0], result, 0.0001);
    Assert.assertEquals(objects[0].longValue(), agg.getLong(buffer, 0));
    Assert.assertEquals(objects[0], agg.getFloat(buffer, 0), 0.0001);
  }

  @Test
  public void testSerde() throws Exception
  {
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    String floatSpecJson = "{\"type\":\"floatAny\",\"name\":\"billy\",\"fieldName\":\"nilly\"}";
    Assert.assertEquals(floatAnyAggFactory, mapper.readValue(floatSpecJson, AggregatorFactory.class));
  }

  private void aggregate(
      Aggregator agg
  )
  {
    agg.aggregate();
    valueSelector.increment();
    objectSelector.increment();
  }

  private void aggregate(
      BufferAggregator agg,
      ByteBuffer buff,
      int position
  )
  {
    agg.aggregate(buff, position);
    valueSelector.increment();
    objectSelector.increment();
  }
}
