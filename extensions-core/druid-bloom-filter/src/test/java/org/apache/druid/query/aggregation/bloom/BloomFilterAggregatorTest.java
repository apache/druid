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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.BloomFilterExtensionModule;
import org.apache.druid.guice.BloomFilterSerializersModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregatorTest;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.dimension.RegexFilteredDimensionSpec;
import org.apache.druid.query.extraction.RegexDimExtractionFn;
import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DoubleColumnSelector;
import org.apache.druid.segment.FloatColumnSelector;
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

public class BloomFilterAggregatorTest extends InitializedNullHandlingTest
{
  private static final String NULLISH = NullHandling.replaceWithDefault() ? "" : null;
  private static final List<String[]> VALUES1 = dimensionValues(
      "a",
      "b",
      "c",
      "a",
      "a",
      NULLISH,
      "b",
      "b",
      "b",
      "b",
      "a",
      "a"
  );
  private static final List<String[]> VALUES2 = dimensionValues(
      "a",
      "b",
      "c",
      "x",
      "a",
      "e",
      "b",
      new String[]{NULLISH, "x"},
      new String[]{"x", NULLISH},
      new String[]{"y", "x"},
      new String[]{"x", "y"},
      new String[]{"x", "y", "a"}
  );
  private static final Double[] DOUBLE_VALUES1 = new Double[]{0.1, 1.5, 18.3, 0.1};
  private static final Float[] FLOAT_VALUES1 = new Float[]{0.4f, 0.8f, 23.2f};
  private static final Long[] LONG_VALUES1 = new Long[]{10241L, 12312355L, 0L, 81L};

  private static final int MAX_NUM_VALUES = 15;

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
      filter1 = new BloomKFilter(MAX_NUM_VALUES);
      filter2 = new BloomKFilter(MAX_NUM_VALUES);
      BloomKFilter combinedValuesFilter = new BloomKFilter(MAX_NUM_VALUES);

      createStringFilter(VALUES1, filter1, combinedValuesFilter);
      createStringFilter(VALUES2, filter2, combinedValuesFilter);

      serializedFilter1 = filterToString(filter1);
      serializedFilter2 = filterToString(filter2);
      serializedCombinedFilter = filterToString(combinedValuesFilter);

      BloomKFilter longFilter = new BloomKFilter(MAX_NUM_VALUES);
      for (long val : LONG_VALUES1) {
        longFilter.addLong(val);
      }
      serializedLongFilter = filterToString(longFilter);

      BloomKFilter floatFilter = new BloomKFilter(MAX_NUM_VALUES);
      for (float val : FLOAT_VALUES1) {
        floatFilter.addFloat(val);
      }
      serializedFloatFilter = filterToString(floatFilter);

      BloomKFilter doubleFilter = new BloomKFilter(MAX_NUM_VALUES);
      for (double val : DOUBLE_VALUES1) {
        doubleFilter.addDouble(val);
      }
      serializedDoubleFilter = filterToString(doubleFilter);

    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private static void createStringFilter(List<String[]> values, BloomKFilter filter, BloomKFilter combinedValuesFilter)
  {
    for (String[] vals : values) {
      for (String val : vals) {
        if (!NullHandling.replaceWithDefault() && val == null) {
          filter.addBytes(null, 0, 0);
          combinedValuesFilter.addBytes(null, 0, 0);
        } else {
          filter.addString(NullHandling.nullToEmptyIfNeeded(val));
          combinedValuesFilter.addString(NullHandling.nullToEmptyIfNeeded(val));
        }
      }
    }
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

  static String filterToString(BloomKFilter bloomKFilter) throws IOException
  {
    return StringUtils.encodeBase64String(BloomFilterSerializersModule.bloomKFilterToBytes(bloomKFilter));
  }

  private final DimensionSpec dimSpec = new DefaultDimensionSpec("dim1", "dim1");
  private BloomFilterAggregatorFactory valueAggregatorFactory;

  public BloomFilterAggregatorTest()
  {
    valueAggregatorFactory = new BloomFilterAggregatorFactory(
        "billy",
        dimSpec,
        MAX_NUM_VALUES
    );
  }


  @Test
  public void testAggregateValues() throws IOException
  {
    DimensionSelector dimSelector = new CardinalityAggregatorTest.TestDimensionSelector(VALUES1, null);
    StringBloomFilterAggregator agg = new StringBloomFilterAggregator(dimSelector, MAX_NUM_VALUES, true);

    for (int i = 0; i < VALUES1.size(); ++i) {
      aggregateDimension(Collections.singletonList(dimSelector), agg);
    }

    BloomKFilter bloomKFilter = BloomKFilter.deserialize(
        (ByteBuffer) valueAggregatorFactory.finalizeComputation(agg.get())
    );
    String serialized = filterToString(bloomKFilter);
    Assert.assertEquals(serializedFilter1, serialized);
  }

  @Test
  public void testAggregateLongValues() throws IOException
  {
    TestLongColumnSelector selector = new TestLongColumnSelector(Arrays.asList(LONG_VALUES1));
    LongBloomFilterAggregator agg = new LongBloomFilterAggregator(selector, MAX_NUM_VALUES, true);

    for (Long ignored : LONG_VALUES1) {
      aggregateColumn(Collections.singletonList(selector), agg);
    }

    BloomKFilter bloomKFilter = BloomKFilter.deserialize(
        (ByteBuffer) valueAggregatorFactory.finalizeComputation(agg.get())
    );
    String serialized = filterToString(bloomKFilter);
    Assert.assertEquals(serializedLongFilter, serialized);
  }

  @Test
  public void testAggregateFloatValues() throws IOException
  {
    TestFloatColumnSelector selector = new TestFloatColumnSelector(Arrays.asList(FLOAT_VALUES1));
    FloatBloomFilterAggregator agg = new FloatBloomFilterAggregator(selector, MAX_NUM_VALUES, true);

    for (Float ignored : FLOAT_VALUES1) {
      aggregateColumn(Collections.singletonList(selector), agg);
    }

    BloomKFilter bloomKFilter = BloomKFilter.deserialize(
        (ByteBuffer) valueAggregatorFactory.finalizeComputation(agg.get())
    );
    String serialized = filterToString(bloomKFilter);
    Assert.assertEquals(serializedFloatFilter, serialized);
  }

  @Test
  public void testAggregateDoubleValues() throws IOException
  {
    TestDoubleColumnSelector selector = new TestDoubleColumnSelector(Arrays.asList(DOUBLE_VALUES1));
    DoubleBloomFilterAggregator agg = new DoubleBloomFilterAggregator(selector, MAX_NUM_VALUES, true);

    for (Double ignored : DOUBLE_VALUES1) {
      aggregateColumn(Collections.singletonList(selector), agg);
    }

    BloomKFilter bloomKFilter = BloomKFilter.deserialize(
        (ByteBuffer) valueAggregatorFactory.finalizeComputation(agg.get())
    );
    String serialized = filterToString(bloomKFilter);
    Assert.assertEquals(serializedDoubleFilter, serialized);
  }

  @Test
  public void testBufferAggregateStringValues() throws IOException
  {
    DimensionSelector dimSelector = new CardinalityAggregatorTest.TestDimensionSelector(VALUES2, null);
    StringBloomFilterAggregator agg = new StringBloomFilterAggregator(dimSelector, MAX_NUM_VALUES, true);

    int maxSize = valueAggregatorFactory.getMaxIntermediateSizeWithNulls();
    ByteBuffer buf = ByteBuffer.allocate(maxSize + 64);
    int pos = 10;
    buf.limit(pos + maxSize);

    agg.init(buf, pos);

    for (int i = 0; i < VALUES2.size(); ++i) {
      bufferAggregateDimension(Collections.singletonList(dimSelector), agg, buf, pos);
    }
    BloomKFilter bloomKFilter = BloomKFilter.deserialize(
        (ByteBuffer) valueAggregatorFactory.finalizeComputation(agg.get(buf, pos))
    );
    String serialized = filterToString(bloomKFilter);
    Assert.assertEquals(serializedFilter2, serialized);
  }

  @Test
  public void testBufferAggregateLongValues() throws IOException
  {
    TestLongColumnSelector selector = new TestLongColumnSelector(Arrays.asList(LONG_VALUES1));
    LongBloomFilterAggregator agg = new LongBloomFilterAggregator(selector, MAX_NUM_VALUES, true);

    int maxSize = valueAggregatorFactory.getMaxIntermediateSizeWithNulls();
    ByteBuffer buf = ByteBuffer.allocate(maxSize + 64);
    int pos = 10;
    buf.limit(pos + maxSize);

    agg.init(buf, pos);

    IntStream.range(0, LONG_VALUES1.length)
             .forEach(i -> bufferAggregateColumn(Collections.singletonList(selector), agg, buf, pos));
    BloomKFilter bloomKFilter = BloomKFilter.deserialize(
        (ByteBuffer) valueAggregatorFactory.finalizeComputation(agg.get(buf, pos))
    );
    String serialized = filterToString(bloomKFilter);
    Assert.assertEquals(serializedLongFilter, serialized);
  }

  @Test
  public void testBufferAggregateFloatValues() throws IOException
  {
    TestFloatColumnSelector selector = new TestFloatColumnSelector(Arrays.asList(FLOAT_VALUES1));
    FloatBloomFilterAggregator agg = new FloatBloomFilterAggregator(selector, MAX_NUM_VALUES, true);

    int maxSize = valueAggregatorFactory.getMaxIntermediateSizeWithNulls();
    ByteBuffer buf = ByteBuffer.allocate(maxSize + 64);
    int pos = 10;
    buf.limit(pos + maxSize);

    agg.init(buf, pos);

    IntStream.range(0, FLOAT_VALUES1.length)
             .forEach(i -> bufferAggregateColumn(Collections.singletonList(selector), agg, buf, pos));
    BloomKFilter bloomKFilter = BloomKFilter.deserialize(
        (ByteBuffer) valueAggregatorFactory.finalizeComputation(agg.get(buf, pos))
    );
    String serialized = filterToString(bloomKFilter);
    Assert.assertEquals(serializedFloatFilter, serialized);
  }

  @Test
  public void testBufferAggregateDoubleValues() throws IOException
  {
    TestDoubleColumnSelector selector = new TestDoubleColumnSelector(Arrays.asList(DOUBLE_VALUES1));
    DoubleBloomFilterAggregator agg = new DoubleBloomFilterAggregator(selector, MAX_NUM_VALUES, true);

    int maxSize = valueAggregatorFactory.getMaxIntermediateSizeWithNulls();
    ByteBuffer buf = ByteBuffer.allocate(maxSize + 64);
    int pos = 10;
    buf.limit(pos + maxSize);

    agg.init(buf, pos);

    IntStream.range(0, DOUBLE_VALUES1.length)
             .forEach(i -> bufferAggregateColumn(Collections.singletonList(selector), agg, buf, pos));
    BloomKFilter bloomKFilter = BloomKFilter.deserialize(
        (ByteBuffer) valueAggregatorFactory.finalizeComputation(agg.get(buf, pos))
    );
    String serialized = filterToString(bloomKFilter);
    Assert.assertEquals(serializedDoubleFilter, serialized);
  }

  @Test
  public void testCombineValues() throws IOException
  {
    DimensionSelector dimSelector1 = new CardinalityAggregatorTest.TestDimensionSelector(VALUES1, null);
    DimensionSelector dimSelector2 = new CardinalityAggregatorTest.TestDimensionSelector(VALUES2, null);

    StringBloomFilterAggregator agg1 = new StringBloomFilterAggregator(dimSelector1, MAX_NUM_VALUES, true);
    StringBloomFilterAggregator agg2 = new StringBloomFilterAggregator(dimSelector2, MAX_NUM_VALUES, true);

    for (int i = 0; i < VALUES1.size(); ++i) {
      aggregateDimension(Collections.singletonList(dimSelector1), agg1);
    }
    for (int i = 0; i < VALUES2.size(); ++i) {
      aggregateDimension(Collections.singletonList(dimSelector2), agg2);
    }

    BloomKFilter combined = BloomKFilter.deserialize(
        (ByteBuffer) valueAggregatorFactory.finalizeComputation(
          valueAggregatorFactory.combine(
              agg1.get(),
              agg2.get()
          )
        )
    );

    String serialized = filterToString(combined);
    Assert.assertEquals(serializedCombinedFilter, serialized);
  }

  @Test
  public void testMergeValues() throws IOException
  {
    final TestBloomFilterBufferColumnSelector mergeDim =
        new TestBloomFilterBufferColumnSelector(
            ImmutableList.of(
                ByteBuffer.wrap(BloomFilterSerializersModule.bloomKFilterToBytes(filter1)),
                ByteBuffer.wrap(BloomFilterSerializersModule.bloomKFilterToBytes(filter2))
            )
        );

    BloomFilterMergeAggregator mergeAggregator =
        new BloomFilterMergeAggregator(mergeDim, MAX_NUM_VALUES, true);

    for (int i = 0; i < 2; ++i) {
      aggregateColumn(Collections.singletonList(mergeDim), mergeAggregator);
    }


    BloomKFilter merged = BloomKFilter.deserialize(
        (ByteBuffer) valueAggregatorFactory.getCombiningFactory().finalizeComputation(mergeAggregator.get())
    );
    String serialized = filterToString(merged);
    Assert.assertEquals(serializedCombinedFilter, serialized);
  }

  @Test
  public void testMergeValuesWithBuffersForGroupByV1() throws IOException
  {
    final TestBloomFilterBufferColumnSelector mergeDim =
        new TestBloomFilterBufferColumnSelector(
            ImmutableList.of(
                ByteBuffer.wrap(BloomFilterSerializersModule.bloomKFilterToBytes(filter1)),
                ByteBuffer.wrap(BloomFilterSerializersModule.bloomKFilterToBytes(filter2))
            )
        );

    BloomFilterMergeAggregator mergeAggregator =
        new BloomFilterMergeAggregator(mergeDim, MAX_NUM_VALUES, true);

    for (int i = 0; i < 2; ++i) {
      aggregateColumn(Collections.singletonList(mergeDim), mergeAggregator);
    }


    BloomKFilter merged = BloomKFilter.deserialize(
        (ByteBuffer) valueAggregatorFactory.getCombiningFactory().finalizeComputation(mergeAggregator.get())
    );
    String serialized = filterToString(merged);
    Assert.assertEquals(serializedCombinedFilter, serialized);
  }

  @Test
  public void testBuferMergeValues() throws IOException
  {
    final TestBloomFilterBufferColumnSelector mergeDim =
        new TestBloomFilterBufferColumnSelector(
            ImmutableList.of(
                ByteBuffer.wrap(BloomFilterSerializersModule.bloomKFilterToBytes(filter1)),
                ByteBuffer.wrap(BloomFilterSerializersModule.bloomKFilterToBytes(filter2))
            )
        );

    BloomFilterMergeAggregator mergeAggregator = new BloomFilterMergeAggregator(mergeDim, MAX_NUM_VALUES, false);

    int maxSize = valueAggregatorFactory.getCombiningFactory().getMaxIntermediateSizeWithNulls();
    ByteBuffer buf = ByteBuffer.allocate(maxSize + 64);
    int pos = 10;
    buf.limit(pos + maxSize);

    mergeAggregator.init(buf, pos);

    for (int i = 0; i < 2; ++i) {
      bufferAggregateColumn(Collections.singletonList(mergeDim), mergeAggregator, buf, pos);
    }

    BloomKFilter merged = BloomKFilter.deserialize(
        (ByteBuffer) valueAggregatorFactory.getCombiningFactory().finalizeComputation(mergeAggregator.get(buf, pos))
    );
    String serialized = filterToString(merged);

    Assert.assertEquals(serializedCombinedFilter, serialized);
  }

  @Test
  public void testSerde() throws Exception
  {
    BloomFilterAggregatorFactory factory = new BloomFilterAggregatorFactory(
        "billy",
        new DefaultDimensionSpec("b", "b"),
        MAX_NUM_VALUES
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
        MAX_NUM_VALUES
    );

    Assert.assertEquals(
        factory2,
        objectMapper.readValue(objectMapper.writeValueAsString(factory2), AggregatorFactory.class)
    );

    BloomFilterAggregatorFactory factory3 = new BloomFilterAggregatorFactory(
        "billy",
        new RegexFilteredDimensionSpec(new DefaultDimensionSpec("a", "a"), ".*"),
        MAX_NUM_VALUES
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

  public static class TestBloomFilterBufferColumnSelector extends SteppableSelector<ByteBuffer>
  {
    public TestBloomFilterBufferColumnSelector(List<ByteBuffer> values)
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
