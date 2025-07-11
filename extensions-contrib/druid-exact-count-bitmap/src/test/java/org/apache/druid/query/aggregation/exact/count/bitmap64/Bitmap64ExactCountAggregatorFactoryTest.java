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

package org.apache.druid.query.aggregation.exact.count.bitmap64;

import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnType;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

public class Bitmap64ExactCountAggregatorFactoryTest
{
  private static final String NAME = "testName";
  private static final String FIELD_NAME = "testFieldName";

  private TestBitmap64ExactCountAggregatorFactory factory;

  // Concrete implementation for testing the abstract class
  private static class TestBitmap64ExactCountAggregatorFactory extends Bitmap64ExactCountAggregatorFactory
  {
    private static final byte CACHE_TYPE_ID = 0x1A; // Using a distinct byte for test

    TestBitmap64ExactCountAggregatorFactory(String name, String fieldName)
    {
      super(name, fieldName);
    }

    @Override
    protected byte getCacheTypeId()
    {
      return CACHE_TYPE_ID;
    }

    @Override
    public Aggregator factorize(ColumnSelectorFactory metricFactory)
    {
      return null;
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
    {
      return null;
    }

    @Override
    public ColumnType getIntermediateType()
    {
      return Bitmap64ExactCountBuildAggregatorFactory.TYPE;
    }

    @Override
    public AggregatorFactory withName(String newName)
    {
      return new TestBitmap64ExactCountAggregatorFactory(newName, getFieldName());
    }
  }

  @BeforeEach
  public void setUp()
  {
    factory = new TestBitmap64ExactCountAggregatorFactory(NAME, FIELD_NAME);
  }

  @Test
  public void testConstructor()
  {
    Assertions.assertEquals(NAME, factory.getName());
    Assertions.assertEquals(FIELD_NAME, factory.getFieldName());
  }

  @Test
  public void testRequiredFields()
  {
    Assertions.assertEquals(Collections.singletonList(FIELD_NAME), factory.requiredFields());
  }

  @Test
  public void testFinalizeComputationNull()
  {
    Assertions.assertNull(factory.finalizeComputation(null));
  }

  @Test
  public void testFinalizeComputation()
  {
    Bitmap64 counter = new RoaringBitmap64Counter();
    counter.add(1L);
    counter.add(2L);
    Assertions.assertEquals(2L, factory.finalizeComputation(counter));
  }

  @Test
  public void testGetComparator()
  {
    Bitmap64 counter1 = new RoaringBitmap64Counter();
    counter1.add(1L);

    Bitmap64 counter2 = new RoaringBitmap64Counter();
    counter2.add(1L);
    counter2.add(2L);

    Assertions.assertTrue(factory.getComparator().compare(counter1, counter2) < 0);
    Assertions.assertTrue(factory.getComparator().compare(counter2, counter1) > 0);
    Assertions.assertTrue(factory.getComparator().compare(null, counter1) < 0);
    Assertions.assertTrue(factory.getComparator().compare(counter1, null) > 0);
  }

  @Test
  public void testGetCombiningFactory()
  {
    AggregatorFactory combiningFactory = factory.getCombiningFactory();
    Assertions.assertInstanceOf(Bitmap64ExactCountMergeAggregatorFactory.class, combiningFactory);
    Assertions.assertEquals(NAME, combiningFactory.getName());
    Assertions.assertEquals(NAME, ((Bitmap64ExactCountMergeAggregatorFactory) combiningFactory).getFieldName());
  }

  @Test
  public void testGetMaxIntermediateSize()
  {
    Assertions.assertEquals(
        Bitmap64ExactCountAggregatorFactory.MAX_INTERMEDIATE_SIZE,
        factory.getMaxIntermediateSize()
    );
  }

  @Test
  public void testEqualsAndHashCode()
  {
    // Test symmetry
    TestBitmap64ExactCountAggregatorFactory factory2 = new TestBitmap64ExactCountAggregatorFactory(
        NAME,
        FIELD_NAME
    );
    Assertions.assertEquals(factory, factory2);
    Assertions.assertEquals(factory.hashCode(), factory2.hashCode());

    // Test different name
    TestBitmap64ExactCountAggregatorFactory factoryDiffName = new TestBitmap64ExactCountAggregatorFactory(
        NAME + "_diff",
        FIELD_NAME
    );
    Assertions.assertNotEquals(factory, factoryDiffName);

    // Test different fieldName
    TestBitmap64ExactCountAggregatorFactory factoryDiffFieldName = new TestBitmap64ExactCountAggregatorFactory(
        NAME,
        FIELD_NAME + "_diff"
    );
    Assertions.assertNotEquals(factory, factoryDiffFieldName);

    // Test different class (even if fields match, if getEffectiveClass is used in parent equals)
    // For Bitmap64ExactCountAggregatorFactory, getClass() is used in equals.
    Bitmap64ExactCountAggregatorFactory anotherConcreteFactory = new Bitmap64ExactCountBuildAggregatorFactory(
        NAME,
        FIELD_NAME
    );
    Assertions.assertNotEquals(
        factory,
        anotherConcreteFactory,
        "Test factory should not be equal to Build factory due to different class"
    );
  }

  @Test
  public void testToString()
  {
    String expected = "TestBitmap64ExactCountAggregatorFactory { name="
                      + NAME
                      + ", fieldName="
                      + FIELD_NAME
                      + " }";
    Assertions.assertEquals(expected, factory.toString());
  }

  @Test
  public void testGetCacheKey()
  {
    byte[] cacheKey1 = factory.getCacheKey();
    TestBitmap64ExactCountAggregatorFactory factory2 = new TestBitmap64ExactCountAggregatorFactory(
        NAME,
        FIELD_NAME
    );
    byte[] cacheKey2 = factory2.getCacheKey();
    Assertions.assertArrayEquals(cacheKey1, cacheKey2);

    TestBitmap64ExactCountAggregatorFactory factoryDiffName = new TestBitmap64ExactCountAggregatorFactory(
        NAME + "_diff",
        FIELD_NAME
    );
    byte[] cacheKeyDiffName = factoryDiffName.getCacheKey();
    Assertions.assertFalse(Arrays.equals(cacheKey1, cacheKeyDiffName));

    TestBitmap64ExactCountAggregatorFactory factoryDiffFieldName = new TestBitmap64ExactCountAggregatorFactory(
        NAME,
        FIELD_NAME + "_diff"
    );
    byte[] cacheKeyDiffFieldName = factoryDiffFieldName.getCacheKey();
    Assertions.assertFalse(Arrays.equals(cacheKey1, cacheKeyDiffFieldName));
  }

  @Test
  public void testCombine()
  {
    Bitmap64 counter1 = new RoaringBitmap64Counter();
    counter1.add(1L);
    counter1.add(2L);

    Bitmap64 counter2 = new RoaringBitmap64Counter();
    counter2.add(2L);
    counter2.add(3L);

    Bitmap64 result = factory.combine(counter1, counter2);
    Assertions.assertEquals(3L, result.getCardinality());
    Assertions.assertSame(counter1, result);

    Bitmap64 counter3 = new RoaringBitmap64Counter();
    counter3.add(4L);
    Bitmap64 resultNullB = factory.combine(counter3, null);
    Assertions.assertSame(counter3, resultNullB);
    Assertions.assertEquals(1L, resultNullB.getCardinality());

    Bitmap64 counter4 = new RoaringBitmap64Counter();
    counter4.add(5L);
    Bitmap64 resultNullA = factory.combine(null, counter4);
    Assertions.assertSame(counter4, resultNullA);
    Assertions.assertEquals(1L, resultNullA.getCardinality());

    Assertions.assertNull(factory.combine(null, null));
  }

  @Test
  public void testMakeAggregateCombiner()
  {
    AggregateCombiner<Bitmap64> combiner = factory.makeAggregateCombiner();
    Assertions.assertNotNull(combiner);

    ColumnValueSelector<Bitmap64> selector =
        EasyMock.createMock(ColumnValueSelector.class);

    Bitmap64 counter1 = new RoaringBitmap64Counter();
    counter1.add(10L);
    counter1.add(20L);

    Bitmap64 counter2 = new RoaringBitmap64Counter();
    counter2.add(20L);
    counter2.add(30L);

    EasyMock.expect(selector.getObject()).andReturn(counter1).times(1);
    EasyMock.replay(selector);
    combiner.fold(selector);
    EasyMock.verify(selector);
    Assertions.assertEquals(2L, combiner.getObject().getCardinality());

    EasyMock.reset(selector);
    EasyMock.expect(selector.getObject()).andReturn(counter2).times(1);
    EasyMock.replay(selector);
    combiner.fold(selector);
    EasyMock.verify(selector);
    Assertions.assertEquals(3L, combiner.getObject().getCardinality());

    EasyMock.reset(selector);
    Bitmap64 counter3 = new RoaringBitmap64Counter();
    counter3.add(40L);
    EasyMock.expect(selector.getObject()).andReturn(counter3).times(1);
    EasyMock.replay(selector);
    combiner.reset(selector);
    EasyMock.verify(selector);
    Assertions.assertEquals(1L, combiner.getObject().getCardinality());

    Assertions.assertEquals(Bitmap64.class, combiner.classOfObject());
  }
}
