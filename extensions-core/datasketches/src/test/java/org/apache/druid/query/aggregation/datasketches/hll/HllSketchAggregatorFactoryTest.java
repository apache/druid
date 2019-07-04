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
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertNull;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

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
    assertEquals(ROUND, target.isRound());
  }

  @Test
  public void testGetRequiredColumns()
  {
    List<AggregatorFactory> aggregatorFactories = target.getRequiredColumns();
    assertEquals(1, aggregatorFactories.size());
    HllSketchAggregatorFactory aggregatorFactory = (HllSketchAggregatorFactory) aggregatorFactories.get(0);
    assertEquals(FIELD_NAME, aggregatorFactory.getName());
    assertEquals(FIELD_NAME, aggregatorFactory.getFieldName());
    assertEquals(LG_K, aggregatorFactory.getLgK());
    assertEquals(TGT_HLL_TYPE, aggregatorFactory.getTgtHllType());
    assertEquals(ROUND, aggregatorFactory.isRound());
  }

  @Test
  public void testFinalizeComputation_null()
  {
    assertNull(target.finalizeComputation(null));
  }

  @Test
  public void testFinalizeComputation_round()
  {
    Object actual = target.finalizeComputation(getMockSketch());
    assertTrue(actual instanceof Long);
    assertEquals(3L, actual);
  }

  private static HllSketch getMockSketch()
  {
    HllSketch sketch = mock(HllSketch.class);
    expect(sketch.getEstimate()).andReturn(ESTIMATE);
    replay(sketch);
    return sketch;
  }

  @Test
  public void testFinalizeComputation_noRound()
  {
    TestHllSketchAggregatorFactory t = new TestHllSketchAggregatorFactory(
        NAME,
        FIELD_NAME,
        LG_K,
        TGT_HLL_TYPE,
        !ROUND
    );
    Object actual = t.finalizeComputation(getMockSketch());
    assertTrue(actual instanceof Double);
    assertEquals(ESTIMATE, actual);
  }

  @Test
  public void testEquals_sameObject()
  {
    assertEquals(target, target);
  }

  @Test
  public void testEquals_otherNull()
  {
    assertNotEquals(target, null);
  }

  @Test
  public void testEquals_otherDiffClass()
  {
    assertNotEquals(target, NAME);
  }

  @Test
  public void testEquals_otherDiffName()
  {
    TestHllSketchAggregatorFactory other = new TestHllSketchAggregatorFactory(
        NAME + "-diff",
        FIELD_NAME,
        LG_K,
        TGT_HLL_TYPE,
        ROUND
    );
    assertNotEquals(target, other);
  }

  @Test
  public void testEquals_otherDiffFieldName()
  {
    TestHllSketchAggregatorFactory other = new TestHllSketchAggregatorFactory(
        NAME,
        FIELD_NAME + "-diff",
        LG_K,
        TGT_HLL_TYPE,
        ROUND
    );
    assertNotEquals(target, other);
  }

  @Test
  public void testEquals_otherDiffLgK()
  {
    TestHllSketchAggregatorFactory other = new TestHllSketchAggregatorFactory(
        NAME,
        FIELD_NAME,
        LG_K + 1,
        TGT_HLL_TYPE,
        ROUND
    );
    assertNotEquals(target, other);
  }

  @Test
  public void testEquals_otherDiffTgtHllType()
  {
    TestHllSketchAggregatorFactory other = new TestHllSketchAggregatorFactory(
        NAME,
        FIELD_NAME,
        LG_K,
        TgtHllType.HLL_8.name(),
        ROUND
    );
    assertNotEquals(target, other);
  }

  @Test
  public void testEquals_otherDiffRound()
  {
    TestHllSketchAggregatorFactory other = new TestHllSketchAggregatorFactory(
        NAME,
        FIELD_NAME,
        LG_K,
        TGT_HLL_TYPE,
        !ROUND
    );
    assertNotEquals(target, other);
  }

  @Test
  public void testEquals_otherMatches()
  {
    TestHllSketchAggregatorFactory other = new TestHllSketchAggregatorFactory(
        NAME,
        FIELD_NAME,
        LG_K,
        TGT_HLL_TYPE,
        ROUND
    );
    assertEquals(target, other);
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
      assertTrue("Missing \"" + expectedToken + "\"", string.contains(expectedToken));
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
