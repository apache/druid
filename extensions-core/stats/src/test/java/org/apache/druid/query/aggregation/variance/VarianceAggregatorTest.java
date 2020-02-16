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

package org.apache.druid.query.aggregation.variance;

import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.aggregation.TestFloatColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 */
public class VarianceAggregatorTest extends InitializedNullHandlingTest
{
  private VarianceAggregatorFactory aggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestFloatColumnSelector selector;

  private final float[] values = {1.1f, 2.7f, 3.5f, 1.3f};
  private final double[] variances_pop = new double[values.length]; // calculated
  private final double[] variances_samp = new double[values.length]; // calculated

  public VarianceAggregatorTest() throws Exception
  {
    String aggSpecJson = "{\"type\": \"variance\", \"name\": \"billy\", \"fieldName\": \"nilly\"}";
    aggFactory = new DefaultObjectMapper().readValue(aggSpecJson, VarianceAggregatorFactory.class);
    double sum = 0;
    for (int i = 0; i < values.length; i++) {
      sum += values[i];
      if (i > 0) {
        double mean = sum / (i + 1);
        double temp = 0;
        for (int j = 0; j <= i; j++) {
          temp += Math.pow(values[j] - mean, 2);
        }
        variances_pop[i] = temp / (i + 1);
        variances_samp[i] = temp / i;
      }
    }
  }

  @Before
  public void setup()
  {
    selector = new TestFloatColumnSelector(values);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(selector);
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("nilly")).andReturn(null);
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testDoubleVarianceAggregator()
  {
    VarianceAggregator agg = (VarianceAggregator) aggFactory.factorize(colSelectorFactory);

    assertValues((VarianceAggregatorCollector) agg.get(), 0, 0d, 0d);
    aggregate(selector, agg);
    assertValues((VarianceAggregatorCollector) agg.get(), 1, 1.1d, 0d);
    aggregate(selector, agg);
    assertValues((VarianceAggregatorCollector) agg.get(), 2, 3.8d, 1.28d);
    aggregate(selector, agg);
    assertValues((VarianceAggregatorCollector) agg.get(), 3, 7.3d, 2.9866d);
    aggregate(selector, agg);
    assertValues((VarianceAggregatorCollector) agg.get(), 4, 8.6d, 3.95d);
  }

  private void assertValues(VarianceAggregatorCollector holder, long count, double sum, double nvariance)
  {
    Assert.assertEquals(count, holder.count);
    Assert.assertEquals(sum, holder.sum, 0.0001);
    Assert.assertEquals(nvariance, holder.nvariance, 0.0001);
    if (count == 0) {
      try {
        holder.getVariance(false);
        Assert.fail("Should throw ISE");
      }
      catch (IllegalStateException e) {
        Assert.assertTrue(e.getMessage().contains("should not be empty holder"));
      }
    } else {
      Assert.assertEquals(holder.getVariance(true), variances_pop[(int) count - 1], 0.0001);
      Assert.assertEquals(holder.getVariance(false), variances_samp[(int) count - 1], 0.0001);
    }
  }

  @Test
  public void testDoubleVarianceBufferAggregator()
  {
    VarianceBufferAggregator agg = (VarianceBufferAggregator) aggFactory.factorizeBuffered(
        colSelectorFactory
    );

    ByteBuffer buffer = ByteBuffer.wrap(new byte[aggFactory.getMaxIntermediateSizeWithNulls()]);
    agg.init(buffer, 0);

    assertValues((VarianceAggregatorCollector) agg.get(buffer, 0), 0, 0d, 0d);
    aggregate(selector, agg, buffer, 0);
    assertValues((VarianceAggregatorCollector) agg.get(buffer, 0), 1, 1.1d, 0d);
    aggregate(selector, agg, buffer, 0);
    assertValues((VarianceAggregatorCollector) agg.get(buffer, 0), 2, 3.8d, 1.28d);
    aggregate(selector, agg, buffer, 0);
    assertValues((VarianceAggregatorCollector) agg.get(buffer, 0), 3, 7.3d, 2.9866d);
    aggregate(selector, agg, buffer, 0);
    assertValues((VarianceAggregatorCollector) agg.get(buffer, 0), 4, 8.6d, 3.95d);
  }

  @Test
  public void testCombine()
  {
    VarianceAggregatorCollector holder1 = new VarianceAggregatorCollector().add(1.1f).add(2.7f);
    VarianceAggregatorCollector holder2 = new VarianceAggregatorCollector().add(3.5f).add(1.3f);
    VarianceAggregatorCollector expected = new VarianceAggregatorCollector(4, 8.6d, 3.95d);
    Assert.assertTrue(expected.equalsWithEpsilon((VarianceAggregatorCollector) aggFactory.combine(holder1, holder2), 0.00001));
  }

  @Test
  public void testEqualsAndHashCode()
  {
    VarianceAggregatorFactory one = new VarianceAggregatorFactory("name1", "fieldName1");
    VarianceAggregatorFactory oneMore = new VarianceAggregatorFactory("name1", "fieldName1");
    VarianceAggregatorFactory two = new VarianceAggregatorFactory("name2", "fieldName2");

    Assert.assertEquals(one.hashCode(), oneMore.hashCode());

    Assert.assertTrue(one.equals(oneMore));
    Assert.assertFalse(one.equals(two));
  }

  private void aggregate(TestFloatColumnSelector selector, VarianceAggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  private void aggregate(
      TestFloatColumnSelector selector,
      VarianceBufferAggregator agg,
      ByteBuffer buff,
      int position
  )
  {
    agg.aggregate(buff, position);
    selector.increment();
  }
}
