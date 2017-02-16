/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.avg;

import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.TestFloatColumnSelector;
import io.druid.query.aggregation.TestObjectColumnSelector;
import io.druid.segment.ColumnSelectorFactory;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 */
public class AvgAggregatorTest
{
  private AvgAggregatorFactory aggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestFloatColumnSelector selector;

  private final float[] values = {1.1f, 2.7f, 3.5f, 1.3f};

  public AvgAggregatorTest() throws Exception
  {
    String aggSpecJson = "{\"type\": \"avg\", \"name\": \"billy\", \"fieldName\": \"nilly\"}";
    aggFactory = new DefaultObjectMapper().readValue(aggSpecJson, AvgAggregatorFactory.class);
  }

  @Before
  public void setup()
  {
    selector = new TestFloatColumnSelector(values);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeObjectColumnSelector("nilly")).andReturn(new TestObjectColumnSelector(0.0f));
    EasyMock.expect(colSelectorFactory.makeFloatColumnSelector("nilly")).andReturn(selector);
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testDoubleAvgAggregator()
  {
    AvgAggregator agg = (AvgAggregator) aggFactory.factorize(colSelectorFactory);

    assertValues((AvgAggregatorCollector) agg.get(), 0, 0d);
    aggregate(selector, agg);
    assertValues((AvgAggregatorCollector) agg.get(), 1, 1.1d);
    aggregate(selector, agg);
    assertValues((AvgAggregatorCollector) agg.get(), 2, 3.8d);
    aggregate(selector, agg);
    assertValues((AvgAggregatorCollector) agg.get(), 3, 7.3d);
    aggregate(selector, agg);
    assertValues((AvgAggregatorCollector) agg.get(), 4, 8.6d);

    agg.reset();
    assertValues((AvgAggregatorCollector) agg.get(), 0, 0d);
  }

  private void assertValues(AvgAggregatorCollector holder, long count, double sum)
  {
    Assert.assertEquals(count, holder.count);
    Assert.assertEquals(sum, holder.sum, 0.0001);
  }

  @Test
  public void testDoubleAvgBufferAggregator()
  {
    AvgBufferAggregator agg = (AvgBufferAggregator) aggFactory.factorizeBuffered(
        colSelectorFactory
    );

    ByteBuffer buffer = ByteBuffer.wrap(new byte[aggFactory.getMaxIntermediateSize()]);
    agg.init(buffer, 0);

    assertValues((AvgAggregatorCollector) agg.get(buffer, 0), 0, 0d);
    aggregate(selector, agg, buffer, 0);
    assertValues((AvgAggregatorCollector) agg.get(buffer, 0), 1, 1.1d);
    aggregate(selector, agg, buffer, 0);
    assertValues((AvgAggregatorCollector) agg.get(buffer, 0), 2, 3.8d);
    aggregate(selector, agg, buffer, 0);
    assertValues((AvgAggregatorCollector) agg.get(buffer, 0), 3, 7.3d);
    aggregate(selector, agg, buffer, 0);
    assertValues((AvgAggregatorCollector) agg.get(buffer, 0), 4, 8.6d);
  }

  @Test
  public void testCombine()
  {
    AvgAggregatorCollector holder1 = new AvgAggregatorCollector().add(1.1f).add(2.7f);
    AvgAggregatorCollector holder2 = new AvgAggregatorCollector().add(3.5f).add(1.3f);
    AvgAggregatorCollector expected = new AvgAggregatorCollector(4, 8.6d);
    Assert.assertTrue(expected.equalsWithEpsilon(
        (AvgAggregatorCollector) aggFactory.combine(holder1, holder2),
        0.00001
    ));
  }

  @Test
  public void testEqualsAndHashCode() throws Exception
  {
    AvgAggregatorFactory one = new AvgAggregatorFactory("name1", "fieldName1");
    AvgAggregatorFactory oneMore = new AvgAggregatorFactory("name1", "fieldName1");
    AvgAggregatorFactory two = new AvgAggregatorFactory("name2", "fieldName2");

    Assert.assertEquals(one.hashCode(), oneMore.hashCode());

    Assert.assertTrue(one.equals(oneMore));
    Assert.assertFalse(one.equals(two));
  }

  private void aggregate(TestFloatColumnSelector selector, AvgAggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  private void aggregate(
      TestFloatColumnSelector selector,
      AvgBufferAggregator agg,
      ByteBuffer buff,
      int position
  )
  {
    agg.aggregate(buff, position);
    selector.increment();
  }
}
