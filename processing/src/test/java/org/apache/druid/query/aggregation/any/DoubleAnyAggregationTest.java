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
import org.apache.druid.query.aggregation.TestDoubleColumnSelectorImpl;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class DoubleAnyAggregationTest extends InitializedNullHandlingTest
{
  private DoubleAnyAggregatorFactory doubleAnyAggFactory;
  private DoubleAnyAggregatorFactory combiningAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestDoubleColumnSelectorImpl valueSelector;
  private TestObjectColumnSelector objectSelector;

  private double[] doubles = {1.1897d, 0.001d, 86.23d, 166.228d};
  private Double[] objects = {2.1897d, 1.001d, 87.23d, 167.228d};

  @BeforeEach
  public void setup()
  {
    doubleAnyAggFactory = new DoubleAnyAggregatorFactory("billy", "nilly");
    combiningAggFactory = (DoubleAnyAggregatorFactory) doubleAnyAggFactory.getCombiningFactory();
    valueSelector = new TestDoubleColumnSelectorImpl(doubles);
    objectSelector = new TestObjectColumnSelector<>(objects);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(valueSelector);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("billy")).andReturn(objectSelector);
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testDoubleAnyAggregator()
  {
    Aggregator agg = doubleAnyAggFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Double result = (Double) agg.get();

    Assertions.assertEquals((Double) doubles[0], result);
    Assertions.assertEquals((long) doubles[0], agg.getLong());
    Assertions.assertEquals(doubles[0], agg.getDouble(), 0.0001);
  }

  @Test
  public void testDoubleAnyBufferAggregator()
  {
    BufferAggregator agg = doubleAnyAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[doubleAnyAggFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Double result = (Double) agg.get(buffer, 0);

    Assertions.assertEquals(doubles[0], result, 0.0001);
    Assertions.assertEquals((long) doubles[0], agg.getLong(buffer, 0));
    Assertions.assertEquals(doubles[0], agg.getDouble(buffer, 0), 0.0001);
  }

  @Test
  public void testCombine()
  {
    Double d1 = 3.0;
    Double d2 = 4.0;
    Assertions.assertEquals(d1, doubleAnyAggFactory.combine(d1, d2));
  }

  @Test
  public void testComparatorWithNulls()
  {
    Double d1 = 3.0;
    Double d2 = null;
    Comparator comparator = doubleAnyAggFactory.getComparator();
    Assertions.assertEquals(1, comparator.compare(d1, d2));
    Assertions.assertEquals(0, comparator.compare(d1, d1));
    Assertions.assertEquals(0, comparator.compare(d2, d2));
    Assertions.assertEquals(-1, comparator.compare(d2, d1));
  }

  @Test
  public void testComparatorWithTypeMismatch()
  {
    Long n1 = 3L;
    Double n2 = 4.0;
    Comparator comparator = doubleAnyAggFactory.getComparator();
    Assertions.assertEquals(0, comparator.compare(n1, n1));
    Assertions.assertEquals(-1, comparator.compare(n1, n2));
    Assertions.assertEquals(1, comparator.compare(n2, n1));
  }

  @Test
  public void testDoubleAnyCombiningAggregator()
  {
    Aggregator agg = combiningAggFactory.factorize(colSelectorFactory);

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Double result = (Double) agg.get();

    Assertions.assertEquals(objects[0], result, 0.0001);
    Assertions.assertEquals(objects[0].longValue(), agg.getLong());
    Assertions.assertEquals(objects[0], agg.getDouble(), 0.0001);
  }

  @Test
  public void testDoubleAnyCombiningBufferAggregator()
  {
    BufferAggregator agg = combiningAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[doubleAnyAggFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Double result = (Double) agg.get(buffer, 0);

    Assertions.assertEquals(objects[0], result, 0.0001);
    Assertions.assertEquals(objects[0].longValue(), agg.getLong(buffer, 0));
    Assertions.assertEquals(objects[0], agg.getDouble(buffer, 0), 0.0001);
  }

  @Test
  public void testSerde() throws Exception
  {
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    String doubleSpecJson = "{\"type\":\"doubleAny\",\"name\":\"billy\",\"fieldName\":\"nilly\"}";
    Assertions.assertEquals(doubleAnyAggFactory, mapper.readValue(doubleSpecJson, AggregatorFactory.class));
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
