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

package org.apache.druid.query.aggregation.datasketches.hll;

import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HllSketchAggregatorFactoryTest
{
  private static final String NAME = "name";
  private static final String FIELD_NAME = "fieldName";
  private static final int LG_K = HllSketchAggregatorFactory.DEFAULT_LG_K;
  private static final String TGT_HLL_TYPE = TgtHllType.HLL_4.name();
  private static final boolean ROUND = true;
  private static final double ESTIMATE = Math.PI;

  private TestHllSketchAggregatorFactory target;

  @Before
  public void setUp()
  {
    target = new TestHllSketchAggregatorFactory(NAME, FIELD_NAME, LG_K, TGT_HLL_TYPE, ROUND);
  }

  @Test
  public void testIsRound()
  {
    Assert.assertEquals(ROUND, target.isRound());
  }

  @Test
  public void testGetRequiredColumns()
  {
    List<AggregatorFactory> aggregatorFactories = target.getRequiredColumns();
    Assert.assertEquals(1, aggregatorFactories.size());
    HllSketchAggregatorFactory aggregatorFactory = (HllSketchAggregatorFactory) aggregatorFactories.get(0);
    Assert.assertEquals(FIELD_NAME, aggregatorFactory.getName());
    Assert.assertEquals(FIELD_NAME, aggregatorFactory.getFieldName());
    Assert.assertEquals(LG_K, aggregatorFactory.getLgK());
    Assert.assertEquals(TGT_HLL_TYPE, aggregatorFactory.getTgtHllType());
    Assert.assertEquals(ROUND, aggregatorFactory.isRound());
  }

  @Test
  public void testFinalizeComputationNull()
  {
    Assert.assertNull(target.finalizeComputation(null));
  }

  @Test
  public void testFinalizeComputationRound()
  {
    Object actual = target.finalizeComputation(getMockSketch());
    Assert.assertTrue(actual instanceof Long);
    Assert.assertEquals(3L, actual);
  }

  private static HllSketch getMockSketch()
  {
    HllSketch sketch = EasyMock.mock(HllSketch.class);
    EasyMock.expect(sketch.getEstimate()).andReturn(ESTIMATE);
    EasyMock.replay(sketch);
    return sketch;
  }

  @Test
  public void testFinalizeComputatioNoRound()
  {
    TestHllSketchAggregatorFactory t = new TestHllSketchAggregatorFactory(
        NAME,
        FIELD_NAME,
        LG_K,
        TGT_HLL_TYPE,
        !ROUND
    );
    Object actual = t.finalizeComputation(getMockSketch());
    Assert.assertTrue(actual instanceof Double);
    Assert.assertEquals(ESTIMATE, actual);
  }

  @Test
  public void testEqualsSameObject()
  {
    Assert.assertEquals(target, target);
  }

  @Test
  public void testEqualsOtherNull()
  {
    Assert.assertNotEquals(target, null);
  }

  @Test
  public void testEqualsOtherDiffClass()
  {
    Assert.assertNotEquals(target, NAME);
  }

  @Test
  public void testEqualsOtherDiffName()
  {
    TestHllSketchAggregatorFactory other = new TestHllSketchAggregatorFactory(
        NAME + "-diff",
        FIELD_NAME,
        LG_K,
        TGT_HLL_TYPE,
        ROUND
    );
    Assert.assertNotEquals(target, other);
  }

  @Test
  public void testEqualsOtherDiffFieldName()
  {
    TestHllSketchAggregatorFactory other = new TestHllSketchAggregatorFactory(
        NAME,
        FIELD_NAME + "-diff",
        LG_K,
        TGT_HLL_TYPE,
        ROUND
    );
    Assert.assertNotEquals(target, other);
  }

  @Test
  public void testEqualsOtherDiffLgK()
  {
    TestHllSketchAggregatorFactory other = new TestHllSketchAggregatorFactory(
        NAME,
        FIELD_NAME,
        LG_K + 1,
        TGT_HLL_TYPE,
        ROUND
    );
    Assert.assertNotEquals(target, other);
  }

  @Test
  public void testEqualsOtherDiffTgtHllType()
  {
    TestHllSketchAggregatorFactory other = new TestHllSketchAggregatorFactory(
        NAME,
        FIELD_NAME,
        LG_K,
        TgtHllType.HLL_8.name(),
        ROUND
    );
    Assert.assertNotEquals(target, other);
  }

  @Test
  public void testEqualsOtherDiffRound()
  {
    TestHllSketchAggregatorFactory other = new TestHllSketchAggregatorFactory(
        NAME,
        FIELD_NAME,
        LG_K,
        TGT_HLL_TYPE,
        !ROUND
    );
    Assert.assertNotEquals(target, other);
  }

  @Test
  public void testEqualsOtherMatches()
  {
    TestHllSketchAggregatorFactory other = new TestHllSketchAggregatorFactory(
        NAME,
        FIELD_NAME,
        LG_K,
        TGT_HLL_TYPE,
        ROUND
    );
    Assert.assertEquals(target, other);
  }

  @Test
  public void testToString()
  {
    String string = target.toString();
    List<Field> toStringFields = Arrays.stream(HllSketchAggregatorFactory.class.getDeclaredFields())
                                       .filter(HllSketchAggregatorFactoryTest::isToStringField)
                                       .collect(Collectors.toList());

    for (Field field : toStringFields) {
      String expectedToken = formatFieldForToString(field);
      Assert.assertTrue("Missing \"" + expectedToken + "\"", string.contains(expectedToken));
    }
  }

  private static boolean isToStringField(Field field)
  {
    int modfiers = field.getModifiers();
    return Modifier.isPrivate(modfiers) && !Modifier.isStatic(modfiers) && Modifier.isFinal(modfiers);
  }

  private static String formatFieldForToString(Field field)
  {
    return " " + field.getName() + "=";
  }

  // Helper for testing abstract base class
  private static class TestHllSketchAggregatorFactory extends HllSketchAggregatorFactory
  {
    private static final byte DUMMY_CACHE_TYPE_ID = 0;
    private static final Aggregator DUMMY_AGGREGATOR = null;
    private static final BufferAggregator DUMMY_BUFFER_AGGREGATOR = null;
    private static final String DUMMY_TYPE_NAME = null;
    private static final int DUMMY_SIZE = 0;

    TestHllSketchAggregatorFactory(
        String name,
        String fieldName,
        @Nullable Integer lgK,
        @Nullable String tgtHllType,
        boolean round
    )
    {
      super(name, fieldName, lgK, tgtHllType, round);
    }

    @Override
    protected byte getCacheTypeId()
    {
      return DUMMY_CACHE_TYPE_ID;
    }

    @Override
    public Aggregator factorize(ColumnSelectorFactory metricFactory)
    {
      return DUMMY_AGGREGATOR;
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
    {
      return DUMMY_BUFFER_AGGREGATOR;
    }

    @Override
    public String getTypeName()
    {
      return DUMMY_TYPE_NAME;
    }

    @Override
    public int getMaxIntermediateSize()
    {
      return DUMMY_SIZE;
    }
  }
}
