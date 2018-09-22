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

package org.apache.druid.query.aggregation.bloom;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.BloomFilterExtensionModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.bloom.types.BloomFilterAggregatorColumnSelectorStrategy;
import org.apache.druid.query.aggregation.bloom.types.DoubleBloomFilterAggregatorColumnSelectorStrategy;
import org.apache.druid.query.aggregation.bloom.types.FloatBloomFilterAggregatorColumnSelectorStrategy;
import org.apache.druid.query.aggregation.bloom.types.LongBloomFilterAggregatorColumnValueSelectorStrategy;
import org.apache.druid.query.aggregation.bloom.types.StringBloomFilterAggregatorColumnSelectorStrategy;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregatorTest;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.dimension.RegexFilteredDimensionSpec;
import org.apache.druid.query.extraction.RegexDimExtractionFn;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DoubleColumnSelector;
import org.apache.druid.segment.FloatColumnSelector;
import org.apache.druid.segment.LongColumnSelector;
import org.apache.hive.common.util.BloomKFilter;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BloomFilterAggregatorTest
{
  private static final String nullish = NullHandling.replaceWithDefault() ? "" : null;
  private static final List<String[]> values1 = dimensionValues(
      "a",
      "b",
      "c",
      "a",
      "a",
      nullish,
      "b",
      "b",
      "b",
      "b",
      "a",
      "a"
  );
  private static final List<String[]> values2 = dimensionValues(
      "a",
      "b",
      "c",
      "x",
      "a",
      "e",
      "b",
      new String[]{nullish, "x"},
      new String[]{"x", nullish},
      new String[]{"y", "x"},
      new String[]{"x", "y"},
      new String[]{"x", "y", "a"}
  );
  private static final Double[] doubleValues1 = new Double[]{0.1, 1.5, 18.3, 0.1};
  private static final Float[] floatValues1 = new Float[]{0.4f, 0.8f, 23.2f};
  private static final Long[] longValues1 = new Long[]{10241L, 12312355L, 0L, 81L};

  private static final int maxNumValues = 15;

  private static BloomKFilter filter1;
  private static BloomKFilter filter2;

  private static String serializedFilter1;
  private static String serializedFilter2;
  private static String serializedCombinedFilter;
  private static String serializedLongFilter;
  private static String serializedDoubleFilter;
  private static String serializedFloatFilter;

  static {
    try {
      filter1 = new BloomKFilter(maxNumValues);
      filter2 = new BloomKFilter(maxNumValues);
      BloomKFilter combinedValuesFilter = new BloomKFilter(maxNumValues);

      for (String[] values : values1) {
        for (String val : values) {
          if (!NullHandling.replaceWithDefault() && val == null) {
            filter1.addBytes(null, 0, 0);
            combinedValuesFilter.addBytes(null, 0, 0);
          } else {
            filter1.addString(NullHandling.nullToEmptyIfNeeded(val));
            combinedValuesFilter.addString(NullHandling.nullToEmptyIfNeeded(val));
          }
        }
      }
      for (String[] values : values2) {
        for (String val : values) {
          if (!NullHandling.replaceWithDefault() && val == null) {
            filter2.addBytes(null, 0, 0);
            combinedValuesFilter.addBytes(null, 0, 0);
          } else {
            filter2.addString(NullHandling.nullToEmptyIfNeeded(val));
            combinedValuesFilter.addString(NullHandling.nullToEmptyIfNeeded(val));
          }
        }
      }

      serializedFilter1 = filterToString(filter1);
      serializedFilter2 = filterToString(filter2);
      serializedCombinedFilter = filterToString(combinedValuesFilter);

      BloomKFilter longFilter = new BloomKFilter(maxNumValues);
      for (long val : longValues1) {
        longFilter.addLong(val);
      }
      serializedLongFilter = filterToString(longFilter);

      BloomKFilter floatFilter = new BloomKFilter(maxNumValues);
      for (float val : floatValues1) {
        floatFilter.addFloat(val);
      }
      serializedFloatFilter = filterToString(floatFilter);

      BloomKFilter doubleFilter = new BloomKFilter(maxNumValues);
      for (double val : doubleValues1) {
        doubleFilter.addDouble(val);
      }
      serializedDoubleFilter = filterToString(doubleFilter);

    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private final DimensionSpec dimSpec = new DefaultDimensionSpec("dim1", "dim1");
  private BloomFilterAggregatorFactory valueAggregatorFactory;
  public BloomFilterAggregatorTest()
  {

    valueAggregatorFactory = new BloomFilterAggregatorFactory(
        "billy",
        dimSpec,
        maxNumValues
    );

  }

  private static List<String[]> dimensionValues(Object... values)
  {
    return Lists.transform(
        Lists.newArrayList(values), new Function<Object, String[]>()
        {
          @Nullable
          @Override
          public String[] apply(@Nullable Object input)
          {
            if (input instanceof String[]) {
              return (String[]) input;
            } else {
              return new String[]{(String) input};
            }
          }
        }
    );
  }

  private static void aggregateDimension(List<DimensionSelector> selectorList, Aggregator agg)
  {
    agg.aggregate();

    for (DimensionSelector selector : selectorList) {
      ((CardinalityAggregatorTest.TestDimensionSelector) selector).increment();
    }
  }

  private static void bufferAggregateDimension(
      List<DimensionSelector> selectorList,
      BufferAggregator agg,
      ByteBuffer buf,
      int pos
  )
  {
    agg.aggregate(buf, pos);

    for (DimensionSelector selector : selectorList) {
      ((CardinalityAggregatorTest.TestDimensionSelector) selector).increment();
    }
  }

  private static void aggregateColumn(List<SteppableSelector> selectorList, Aggregator agg)
  {
    agg.aggregate();

    for (SteppableSelector selector : selectorList) {
      selector.increment();
    }
  }

  private static void bufferAggregateColumn(
      List<SteppableSelector> selectorList,
      BufferAggregator agg,
      ByteBuffer buf,
      int pos
  )
  {
    agg.aggregate(buf, pos);

    for (SteppableSelector selector : selectorList) {
      selector.increment();
    }
  }

  private static String filterToString(BloomKFilter bloomKFilter) throws IOException
  {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    BloomKFilter.serialize(byteArrayOutputStream, bloomKFilter);
    byte[] bytes = byteArrayOutputStream.toByteArray();
    return Base64.encodeBase64String(bytes);
  }

  @Test
  public void testAggregateValues() throws IOException
  {
    DimensionSelector dimSelector = new CardinalityAggregatorTest.TestDimensionSelector(values1, null);
    BloomFilterAggregator agg = new BloomFilterAggregator(
        new ColumnSelectorPlus<>(
            dimSpec.getDimension(),
            dimSpec.getOutputName(),
            new StringBloomFilterAggregatorColumnSelectorStrategy(),
            dimSelector
        ),
        maxNumValues
    );

    for (int i = 0; i < values1.size(); ++i) {
      aggregateDimension(Collections.singletonList(dimSelector), agg);
    }

    BloomKFilter bloomKFilter = (BloomKFilter) valueAggregatorFactory.finalizeComputation(agg.get());
    String serialized = filterToString(bloomKFilter);
    Assert.assertEquals(serializedFilter1, serialized);
  }

  @Test
  public void testAggregateLongValues() throws IOException
  {
    TestLongColumnSelector selector = new TestLongColumnSelector(Arrays.asList(longValues1));
    BloomFilterAggregator agg = new BloomFilterAggregator(
        new ColumnSelectorPlus<>(
            "longColumn",
            "longlongman",
            new LongBloomFilterAggregatorColumnValueSelectorStrategy(),
            selector
        ),
        maxNumValues
    );

    for (Long ignored : longValues1) {
      aggregateColumn(Collections.singletonList(selector), agg);
    }

    BloomKFilter bloomKFilter = (BloomKFilter) valueAggregatorFactory.finalizeComputation(agg.get());
    String serialized = filterToString(bloomKFilter);
    Assert.assertEquals(serializedLongFilter, serialized);
  }

  @Test
  public void testAggregateFloatValues() throws IOException
  {
    TestFloatColumnSelector selector = new TestFloatColumnSelector(Arrays.asList(floatValues1));
    BloomFilterAggregator agg = new BloomFilterAggregator(
        new ColumnSelectorPlus<>(
            "floatColumn",
            "floatColumn",
            new FloatBloomFilterAggregatorColumnSelectorStrategy(),
            selector
        ),
        maxNumValues
    );

    for (Float ignored : floatValues1) {
      aggregateColumn(Collections.singletonList(selector), agg);
    }

    BloomKFilter bloomKFilter = (BloomKFilter) valueAggregatorFactory.finalizeComputation(agg.get());
    String serialized = filterToString(bloomKFilter);
    Assert.assertEquals(serializedFloatFilter, serialized);
  }

  @Test
  public void testAggregateDoubleValues() throws IOException
  {
    TestDoubleColumnSelector selector = new TestDoubleColumnSelector(Arrays.asList(doubleValues1));
    BloomFilterAggregator agg = new BloomFilterAggregator(
        new ColumnSelectorPlus<>(
            "doubleColumn",
            "doubleColumn",
            new DoubleBloomFilterAggregatorColumnSelectorStrategy(),
            selector
        ),
        maxNumValues
    );

    for (Double ignored : doubleValues1) {
      aggregateColumn(Collections.singletonList(selector), agg);
    }

    BloomKFilter bloomKFilter = (BloomKFilter) valueAggregatorFactory.finalizeComputation(agg.get());
    String serialized = filterToString(bloomKFilter);
    Assert.assertEquals(serializedDoubleFilter, serialized);
  }

  @Test
  public void testBufferAggregateValues() throws IOException
  {
    DimensionSelector dimSelector = new CardinalityAggregatorTest.TestDimensionSelector(values2, null);
    BloomFilterBufferAggregator agg = new BloomFilterBufferAggregator(
        new ColumnSelectorPlus<>(
            dimSpec.getDimension(),
            dimSpec.getOutputName(),
            new StringBloomFilterAggregatorColumnSelectorStrategy(),
            dimSelector
        ),
        maxNumValues
    );

    int maxSize = valueAggregatorFactory.getMaxIntermediateSizeWithNulls();
    ByteBuffer buf = ByteBuffer.allocate(maxSize + 64);
    int pos = 10;
    buf.limit(pos + maxSize);

    agg.init(buf, pos);

    for (int i = 0; i < values2.size(); ++i) {
      bufferAggregateDimension(Collections.singletonList(dimSelector), agg, buf, pos);
    }
    BloomKFilter bloomKFilter = (BloomKFilter) valueAggregatorFactory.finalizeComputation(agg.get(buf, pos));
    String serialized = filterToString(bloomKFilter);
    Assert.assertEquals(serializedFilter2, serialized);
  }

  @Test
  public void testCombineValues() throws IOException
  {
    DimensionSelector dimSelector1 = new CardinalityAggregatorTest.TestDimensionSelector(values1, null);
    DimensionSelector dimSelector2 = new CardinalityAggregatorTest.TestDimensionSelector(values2, null);

    ColumnSelectorPlus<BloomFilterAggregatorColumnSelectorStrategy> selector1 = new ColumnSelectorPlus<>(
        dimSpec.getDimension(),
        dimSpec.getOutputName(),
        new StringBloomFilterAggregatorColumnSelectorStrategy(),
        dimSelector1
    );

    ColumnSelectorPlus<BloomFilterAggregatorColumnSelectorStrategy> selector2 = new ColumnSelectorPlus<>(
        dimSpec.getDimension(),
        dimSpec.getOutputName(),
        new StringBloomFilterAggregatorColumnSelectorStrategy(),
        dimSelector2
    );

    BloomFilterAggregator agg1 = new BloomFilterAggregator(
        selector1,
        maxNumValues
    );
    BloomFilterAggregator agg2 = new BloomFilterAggregator(
        selector2,
        maxNumValues
    );

    for (int i = 0; i < values1.size(); ++i) {
      aggregateDimension(Collections.singletonList(dimSelector1), agg1);
    }
    for (int i = 0; i < values2.size(); ++i) {
      aggregateDimension(Collections.singletonList(dimSelector2), agg2);
    }

    BloomKFilter combined = (BloomKFilter) valueAggregatorFactory.finalizeComputation(
        valueAggregatorFactory.combine(
            agg1.get(),
            agg2.get()
        )
    );

    String serialized = filterToString(combined);
    Assert.assertEquals(serializedCombinedFilter, serialized);
  }

  @Test
  public void testMergeValues() throws IOException
  {
    final TestBloomFilterColumnSelector mergeDim =
        new TestBloomFilterColumnSelector(ImmutableList.of(filter1, filter2));

    BloomFilterMergeAggregator mergeAggregator = new BloomFilterMergeAggregator(mergeDim, maxNumValues);

    for (int i = 0; i < 2; ++i) {
      aggregateColumn(Collections.singletonList(mergeDim), mergeAggregator);
    }


    BloomKFilter merged = (BloomKFilter) valueAggregatorFactory.getCombiningFactory()
                                                               .finalizeComputation(mergeAggregator.get());
    String serialized = filterToString(merged);
    Assert.assertEquals(serializedCombinedFilter, serialized);
  }

  @Test
  public void testBuferMergeValues() throws IOException
  {
    final TestBloomFilterColumnSelector mergeDim =
        new TestBloomFilterColumnSelector(ImmutableList.of(filter1, filter2));

    BloomFilterMergeBufferAggregator mergeAggregator = new BloomFilterMergeBufferAggregator(mergeDim, maxNumValues);

    int maxSize = valueAggregatorFactory.getCombiningFactory().getMaxIntermediateSizeWithNulls();
    ByteBuffer buf = ByteBuffer.allocate(maxSize + 64);
    int pos = 10;
    buf.limit(pos + maxSize);

    mergeAggregator.init(buf, pos);

    for (int i = 0; i < 2; ++i) {
      bufferAggregateColumn(Collections.singletonList(mergeDim), mergeAggregator, buf, pos);
    }

    BloomKFilter merged = (BloomKFilter) valueAggregatorFactory.getCombiningFactory()
                                                               .finalizeComputation(mergeAggregator.get(buf, pos));
    String serialized = filterToString(merged);

    Assert.assertEquals(serializedCombinedFilter, serialized);
  }

  @Test
  public void testSerde() throws Exception
  {
    BloomFilterAggregatorFactory factory = new BloomFilterAggregatorFactory(
        "billy",
        new DefaultDimensionSpec("b", "b"),
        maxNumValues
    );
    ObjectMapper objectMapper = new DefaultObjectMapper();
    new BloomFilterExtensionModule().getJacksonModules().forEach(objectMapper::registerModule);
    Assert.assertEquals(
        factory,
        objectMapper.readValue(objectMapper.writeValueAsString(factory), AggregatorFactory.class)
    );

    String fieldNamesOnly = "{"
                            + "\"type\":\"bloom\","
                            + "\"name\":\"billy\","
                            + "\"field\":\"b\","
                            + "\"maxNumEntries\":15"
                            + "}";
    Assert.assertEquals(
        factory,
        objectMapper.readValue(fieldNamesOnly, AggregatorFactory.class)
    );

    BloomFilterAggregatorFactory factory2 = new BloomFilterAggregatorFactory(
        "billy",
        new ExtractionDimensionSpec("b", "b", new RegexDimExtractionFn(".*", false, null)),
        maxNumValues
    );

    Assert.assertEquals(
        factory2,
        objectMapper.readValue(objectMapper.writeValueAsString(factory2), AggregatorFactory.class)
    );

    BloomFilterAggregatorFactory factory3 = new BloomFilterAggregatorFactory(
        "billy",
        new RegexFilteredDimensionSpec(new DefaultDimensionSpec("a", "a"), ".*"),
        maxNumValues
    );
    Assert.assertEquals(
        factory3,
        objectMapper.readValue(objectMapper.writeValueAsString(factory3), AggregatorFactory.class)
    );
  }

  private abstract static class SteppableSelector<T> implements ColumnValueSelector<T>
  {
    List<T> values;
    int pos;

    public SteppableSelector(List<T> values)
    {
      this.values = values;
      this.pos = 0;
    }

    @Nullable
    @Override
    public T getObject()
    {
      return values.get(pos);
    }

    public void increment()
    {
      pos++;
    }

    public void reset()
    {
      pos = 0;
    }


    @Override
    public double getDouble()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLong()
    {
      throw new UnsupportedOperationException();
    }


    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {

    }

    @Override
    public Class<T> classOfObject()
    {
      return null;
    }

    @Override
    public boolean isNull()
    {
      return false;
    }
  }

  public static class TestBloomFilterColumnSelector extends SteppableSelector<BloomKFilter>
  {
    public TestBloomFilterColumnSelector(List<BloomKFilter> values)
    {
      super(values);
    }
  }

  public static class TestLongColumnSelector extends SteppableSelector<Long> implements LongColumnSelector
  {
    public TestLongColumnSelector(List<Long> values)
    {
      super(values);
    }

    @Override
    public long getLong()
    {
      return values.get(pos);
    }
  }

  public static class TestFloatColumnSelector extends SteppableSelector<Float> implements FloatColumnSelector
  {
    public TestFloatColumnSelector(List<Float> values)
    {
      super(values);
    }

    @Override
    public float getFloat()
    {
      return values.get(pos);
    }
  }

  public static class TestDoubleColumnSelector extends SteppableSelector<Double> implements DoubleColumnSelector
  {
    public TestDoubleColumnSelector(List<Double> values)
    {
      super(values);
    }

    @Override
    public double getDouble()
    {
      return values.get(pos);
    }
  }
}
