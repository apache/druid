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

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.druid.java.util.common.StringEncoding;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class HllSketchAggregatorFactoryTest
{
  private static final String NAME = "name";
  private static final String FIELD_NAME = "fieldName";
  private static final int LG_K = HllSketchAggregatorFactory.DEFAULT_LG_K;
  private static final String TGT_HLL_TYPE = TgtHllType.HLL_4.name();
  private static final StringEncoding STRING_ENCODING = StringEncoding.UTF16LE;
  private static final boolean ROUND = true;
  private static final double ESTIMATE = Math.PI;

  private TestHllSketchAggregatorFactory target;

  @Before
  public void setUp()
  {
    target = new TestHllSketchAggregatorFactory(NAME, FIELD_NAME, LG_K, TGT_HLL_TYPE, STRING_ENCODING, ROUND);
  }

  @Test
  public void testIsRound()
  {
    Assert.assertEquals(ROUND, target.isRound());
  }

  @Test
  public void testStringEncoding()
  {
    Assert.assertEquals(STRING_ENCODING, target.getStringEncoding());
  }

  @Test
  public void testFinalizeComputationNull()
  {
    Assert.assertEquals(0.0D, target.finalizeComputation(null));
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
        STRING_ENCODING,
        !ROUND
    );
    Object actual = t.finalizeComputation(getMockSketch());
    Assert.assertTrue(actual instanceof Double);
    Assert.assertEquals(ESTIMATE, actual);
  }

  @Test
  public void testEqualsSameObject()
  {
    //noinspection EqualsWithItself
    Assert.assertEquals(target, target);
    Assert.assertArrayEquals(target.getCacheKey(), target.getCacheKey());
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
        STRING_ENCODING,
        ROUND
    );
    Assert.assertNotEquals(target, other);
    Assert.assertFalse(Arrays.equals(target.getCacheKey(), other.getCacheKey()));
  }

  @Test
  public void testEqualsOtherDiffFieldName()
  {
    TestHllSketchAggregatorFactory other = new TestHllSketchAggregatorFactory(
        NAME,
        FIELD_NAME + "-diff",
        LG_K,
        TGT_HLL_TYPE,
        STRING_ENCODING,
        ROUND
    );
    Assert.assertNotEquals(target, other);
    Assert.assertFalse(Arrays.equals(target.getCacheKey(), other.getCacheKey()));
  }

  @Test
  public void testEqualsOtherDiffLgK()
  {
    TestHllSketchAggregatorFactory other = new TestHllSketchAggregatorFactory(
        NAME,
        FIELD_NAME,
        LG_K + 1,
        TGT_HLL_TYPE,
        STRING_ENCODING,
        ROUND
    );
    Assert.assertNotEquals(target, other);
    Assert.assertFalse(Arrays.equals(target.getCacheKey(), other.getCacheKey()));
  }

  @Test
  public void testEqualsOtherDiffTgtHllType()
  {
    TestHllSketchAggregatorFactory other = new TestHllSketchAggregatorFactory(
        NAME,
        FIELD_NAME,
        LG_K,
        TgtHllType.HLL_8.name(),
        STRING_ENCODING,
        ROUND
    );
    Assert.assertNotEquals(target, other);
    Assert.assertFalse(Arrays.equals(target.getCacheKey(), other.getCacheKey()));
  }

  @Test
  public void testEqualsOtherDiffRound()
  {
    TestHllSketchAggregatorFactory other = new TestHllSketchAggregatorFactory(
        NAME,
        FIELD_NAME,
        LG_K,
        TGT_HLL_TYPE,
        STRING_ENCODING,
        !ROUND
    );
    Assert.assertNotEquals(target, other);

    // Rounding does not affect per-segment results, so it does not affect cache key
    Assert.assertArrayEquals(target.getCacheKey(), other.getCacheKey());
  }

  @Test
  public void testEqualsOtherMatches()
  {
    TestHllSketchAggregatorFactory other = new TestHllSketchAggregatorFactory(
        NAME,
        FIELD_NAME,
        LG_K,
        TGT_HLL_TYPE,
        STRING_ENCODING,
        ROUND
    );
    Assert.assertEquals(target, other);
    Assert.assertArrayEquals(target.getCacheKey(), other.getCacheKey());
  }

  @Test
  public void testToString()
  {
    String string = target.toString();
    List<Field> toStringFields = Arrays.stream(HllSketchAggregatorFactory.class.getDeclaredFields())
                                       .filter(HllSketchAggregatorFactoryTest::isToStringField)
                                       .collect(Collectors.toList());

    for (Field field : toStringFields) {
      if ("shouldFinalize".equals(field.getName()) || "stringEncoding".equals(field.getName())) {
        // Skip; not included in the toString if it has the default value.
        continue;
      }

      Pattern expectedPattern = testPatternForToString(field);
      Assert.assertTrue("Missing \"" + field.getName() + "\"", expectedPattern.matcher(string).find());
    }
  }

  @Test
  public void testResultArraySignature()
  {
    final TimeseriesQuery query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2000/3000")
              .granularity(Granularities.HOUR)
              .aggregators(
                  new CountAggregatorFactory("count"),
                  new HllSketchBuildAggregatorFactory(
                      "hllBuild",
                      "col",
                      null,
                      null,
                      null,
                      null,
                      false
                  ),
                  new HllSketchBuildAggregatorFactory(
                      "hllBuildRound",
                      "col",
                      null,
                      null,
                      null,
                      null,
                      true
                  ),
                  new HllSketchMergeAggregatorFactory(
                      "hllMerge",
                      "col",
                      null,
                      null,
                      null,
                      null,
                      false
                  ),
                  new HllSketchMergeAggregatorFactory(
                      "hllMergeRound",
                      "col",
                      null,
                      null,
                      null,
                      null,
                      true
                  ),
                  new HllSketchMergeAggregatorFactory(
                      "hllMergeNoFinalize",
                      "col",
                      null,
                      null,
                      null,
                      false,
                      false
                  )
              )
              .postAggregators(
                  new FieldAccessPostAggregator("hllBuild-access", "hllBuild"),
                  new FinalizingFieldAccessPostAggregator("hllBuild-finalize", "hllBuild"),
                  new FieldAccessPostAggregator("hllBuildRound-access", "hllBuildRound"),
                  new FinalizingFieldAccessPostAggregator("hllBuildRound-finalize", "hllBuildRound"),
                  new FieldAccessPostAggregator("hllMerge-access", "hllMerge"),
                  new FinalizingFieldAccessPostAggregator("hllMerge-finalize", "hllMerge"),
                  new FieldAccessPostAggregator("hllMergeRound-access", "hllMergeRound"),
                  new FinalizingFieldAccessPostAggregator("hllMergeRound-finalize", "hllMergeRound"),
                  new FieldAccessPostAggregator("hllMergeNoFinalize-access", "hllMergeNoFinalize"),
                  new FinalizingFieldAccessPostAggregator("hllMergeNoFinalize-finalize", "hllMergeNoFinalize"),
                  new HllSketchToEstimatePostAggregator(
                      "hllMergeNoFinalize-estimate",
                      new FieldAccessPostAggregator(null, "hllMergeNoFinalize"),
                      false
                  )
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("count", ColumnType.LONG)
                    .add("hllBuild", null)
                    .add("hllBuildRound", null)
                    .add("hllMerge", null)
                    .add("hllMergeRound", null)
                    .add("hllMergeNoFinalize", HllSketchMergeAggregatorFactory.TYPE)
                    .add("hllBuild-access", HllSketchBuildAggregatorFactory.TYPE)
                    .add("hllBuild-finalize", ColumnType.DOUBLE)
                    .add("hllBuildRound-access", HllSketchBuildAggregatorFactory.TYPE)
                    .add("hllBuildRound-finalize", ColumnType.LONG)
                    .add("hllMerge-access", HllSketchMergeAggregatorFactory.TYPE)
                    .add("hllMerge-finalize", ColumnType.DOUBLE)
                    .add("hllMergeRound-access", HllSketchMergeAggregatorFactory.TYPE)
                    .add("hllMergeRound-finalize", ColumnType.LONG)
                    .add("hllMergeNoFinalize-access", HllSketchMergeAggregatorFactory.TYPE)
                    .add("hllMergeNoFinalize-finalize", HllSketchMergeAggregatorFactory.TYPE)
                    .add("hllMergeNoFinalize-estimate", ColumnType.DOUBLE)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }

  @Test
  public void testFoldWithNullObject()
  {
    TestHllSketchAggregatorFactory factory = new TestHllSketchAggregatorFactory(
        NAME,
        FIELD_NAME,
        LG_K,
        TGT_HLL_TYPE,
        STRING_ENCODING,
        !ROUND
    );
    AggregateCombiner aggregateCombiner = factory.makeAggregateCombiner();
    TestObjectColumnSelector objectColumnSelector = new TestObjectColumnSelector(new Object[] {null});
    aggregateCombiner.fold(objectColumnSelector);
  }

  private static boolean isToStringField(Field field)
  {
    int modfiers = field.getModifiers();
    return Modifier.isPrivate(modfiers) && !Modifier.isStatic(modfiers) && Modifier.isFinal(modfiers);
  }

  private static Pattern testPatternForToString(Field field)
  {
    return Pattern.compile("\\b" + Pattern.quote(field.getName()) + "=");
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
        @Nullable StringEncoding stringEncoding,
        boolean round
    )
    {
      super(name, fieldName, lgK, tgtHllType, stringEncoding, null, round);
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
    public ColumnType getIntermediateType()
    {
      return new ColumnType(ValueType.COMPLEX, DUMMY_TYPE_NAME, null);
    }

    @Override
    public int getMaxIntermediateSize()
    {
      return DUMMY_SIZE;
    }

    @Override
    public AggregatorFactory withName(String newName)
    {
      return new TestHllSketchAggregatorFactory(
          newName,
          getFieldName(),
          getLgK(),
          getTgtHllType(),
          getStringEncoding(),
          isRound()
      );
    }
  }
}
