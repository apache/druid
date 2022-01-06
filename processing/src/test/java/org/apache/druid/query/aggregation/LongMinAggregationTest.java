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
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
  */
public class LongMinAggregationTest
{
  private LongMinAggregatorFactory longMinAggFactory;
  private LongMinAggregatorFactory longMinVectorAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private VectorColumnSelectorFactory vectorColumnSelectorFactory;
  private TestLongColumnSelector selector;

  private final long[] values = {-9223372036854775802L, -9223372036854775803L, -9223372036854775806L, -9223372036854775805L};
  private final long[] longValues1 = {5L, 2L, 4L, 100L, 1L, 5L, -2L, -3L, 0L, 55L};

  public LongMinAggregationTest() throws Exception
  {
    String aggSpecJson = "{\"type\": \"longMin\", \"name\": \"billy\", \"fieldName\": \"nilly\"}";
    longMinAggFactory = TestHelper.makeJsonMapper().readValue(aggSpecJson, LongMinAggregatorFactory.class);

    String vectorAggSpecJson = "{\"type\": \"longMin\", \"name\": \"lng\", \"fieldName\": \"lngFld\"}";
    longMinVectorAggFactory = TestHelper.makeJsonMapper().readValue(vectorAggSpecJson, LongMinAggregatorFactory.class);
  }

  @Before
  public void setup()
  {
    NullHandling.initializeForTests();
    selector = new TestLongColumnSelector(values);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector("nilly")).andReturn(selector);
    EasyMock.expect(colSelectorFactory.getColumnCapabilities("nilly")).andReturn(null);
    EasyMock.replay(colSelectorFactory);

    VectorValueSelector vectorValueSelector = EasyMock.createMock(VectorValueSelector.class);
    EasyMock.expect(vectorValueSelector.getLongVector()).andReturn(longValues1).anyTimes();
    EasyMock.expect(vectorValueSelector.getNullVector()).andReturn(null).anyTimes();
    EasyMock.replay(vectorValueSelector);

    vectorColumnSelectorFactory = EasyMock.createMock(VectorColumnSelectorFactory.class);
    EasyMock.expect(vectorColumnSelectorFactory.getColumnCapabilities("lngFld"))
            .andReturn(new ColumnCapabilitiesImpl().setType(ColumnType.LONG).setDictionaryEncoded(true)).anyTimes();
    EasyMock.expect(vectorColumnSelectorFactory.makeValueSelector("lngFld")).andReturn(vectorValueSelector).anyTimes();
    EasyMock.replay(vectorColumnSelectorFactory);
  }

  @Test
  public void testLongMinAggregator()
  {
    Aggregator agg = longMinAggFactory.factorize(colSelectorFactory);

    aggregate(selector, agg);
    aggregate(selector, agg);
    aggregate(selector, agg);
    aggregate(selector, agg);

    Assert.assertEquals(values[2], ((Long) agg.get()).longValue());
    Assert.assertEquals(values[2], agg.getLong());
    Assert.assertEquals((float) values[2], agg.getFloat(), 0.0001);
  }

  @Test
  public void testLongMinBufferAggregator()
  {
    BufferAggregator agg = longMinAggFactory.factorizeBuffered(colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[Long.BYTES + Byte.BYTES]);
    agg.init(buffer, 0);

    aggregate(selector, agg, buffer, 0);
    aggregate(selector, agg, buffer, 0);
    aggregate(selector, agg, buffer, 0);
    aggregate(selector, agg, buffer, 0);

    Assert.assertEquals(values[2], ((Long) agg.get(buffer, 0)).longValue());
    Assert.assertEquals(values[2], agg.getLong(buffer, 0));
    Assert.assertEquals((float) values[2], agg.getFloat(buffer, 0), 0.0001);
  }

  @Test
  public void testLongMinVectorAggregator()
  {
    // Some sanity.
    Assert.assertTrue(longMinVectorAggFactory.canVectorize(vectorColumnSelectorFactory));
    VectorValueSelector vectorValueSelector = longMinVectorAggFactory.vectorSelector(vectorColumnSelectorFactory);
    Assert.assertEquals(longValues1, vectorValueSelector.getLongVector());

    VectorAggregator vectorAggregator = longMinVectorAggFactory.factorizeVector(vectorColumnSelectorFactory);

    final ByteBuffer buf = ByteBuffer.allocate(longMinAggFactory.getMaxIntermediateSizeWithNulls() * 2);
    vectorAggregator.init(buf, 0);

    vectorAggregator.aggregate(buf, 0, 0, 3);
    Assert.assertEquals(longValues1[1], (long) vectorAggregator.get(buf, 0));

    vectorAggregator.aggregate(buf, 1, 0, 3);
    Assert.assertEquals(longValues1[1], (long) vectorAggregator.get(buf, 1));

    vectorAggregator.aggregate(buf, 2, 3, 7);
    Assert.assertEquals(longValues1[6], (long) vectorAggregator.get(buf, 2));

    vectorAggregator.aggregate(buf, 0, 0, 10);
    Assert.assertEquals(longValues1[7], (long) vectorAggregator.get(buf, 0));
  }


  @Test
  public void testCombine()
  {
    Assert.assertEquals(-9223372036854775803L, longMinAggFactory.combine(-9223372036854775800L, -9223372036854775803L));
  }

  @Test
  public void testEqualsAndHashCode()
  {
    LongMinAggregatorFactory one = new LongMinAggregatorFactory("name1", "fieldName1");
    LongMinAggregatorFactory oneMore = new LongMinAggregatorFactory("name1", "fieldName1");
    LongMinAggregatorFactory two = new LongMinAggregatorFactory("name2", "fieldName2");

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
}
