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

package org.apache.druid.query.aggregation.cardinality.accurate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.LongRoaringBitmapCollectorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.JavaScriptExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ZeroIndexedInts;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class AccurateCardinalityAggregatorTest
{

  private LongRoaringBitmapCollectorFactory roaringBitmapCollectorFactory = new LongRoaringBitmapCollectorFactory();

  interface ISeek
  {
    void increment();

    void reset();
  }

  abstract static class AbstractDimensionSelector implements DimensionSelector, ISeek
  {
    protected int pos = 0;

    @Override
    public void increment()
    {
      pos++;
    }

    @Override
    public void reset()
    {
      pos = 0;
    }
  }

  public static class TestStringDimensionSelector extends AbstractDimensionSelector
  {

    private final List<Integer[]> column;
    private final Map<String, Integer> ids;
    private final Map<Integer, String> lookup;
    private final ExtractionFn exFn;


    public TestStringDimensionSelector(Iterable<String[]> values, ExtractionFn exFn)
    {
      this.lookup = new HashMap<>();
      this.ids = new HashMap<>();
      this.exFn = exFn;

      int index = 0;
      for (String[] multiValue : values) {
        for (String value : multiValue) {
          if (!ids.containsKey(value)) {
            ids.put(value, index);
            lookup.put(index, value);
            index++;
          }
        }
      }

      this.column = Lists.newArrayList(
          StreamSupport.stream(values.spliterator(), false).map(input -> Iterators.toArray(
              Iterators.transform(
                  Iterators.forArray(input), new Function<String, Integer>()
                  {
                    @Nullable
                    @Override
                    public Integer apply(@Nullable String input)
                    {
                      return ids.get(input);
                    }
                  }
              ), Integer.class
          )).collect(Collectors.toList())
      );
    }

    @Override
    public IndexedInts getRow()
    {
      final int p = this.pos;
      return new IndexedInts()
      {
        @Override
        public int size()
        {
          return column.get(p).length;
        }

        @Override
        public int get(int i)
        {
          return column.get(p)[i];
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          // Don't care about runtime shape in tests
        }
      };
    }

    @Override
    public ValueMatcher makeValueMatcher(@Nullable String value)
    {
      return DimensionSelectorUtils.makeValueMatcherGeneric(this, value);
    }

    @Override
    public ValueMatcher makeValueMatcher(Predicate<String> predicate)
    {
      return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicate);
    }

    @Override
    public int getValueCardinality()
    {
      return 1;
    }

    @Nullable
    @Override
    public String lookupName(int id)
    {
      String val = lookup.get(id);
      return exFn == null ? val : exFn.apply(val);
    }

    @Override
    public boolean nameLookupPossibleInAdvance()
    {
      return true;
    }

    @Nullable
    @Override
    public IdLookup idLookup()
    {
      return new IdLookup()
      {
        @Override
        public int lookupId(String s)
        {
          return ids.get(s);
        }
      };
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      // Don't care about runtime shape in tests
    }

    @Nullable
    @Override
    public Object getObject()
    {
      return null;
    }

    @Override
    public Class classOfObject()
    {
      return null;
    }
  }


  public static class TestLongDimensionSelector extends AbstractDimensionSelector implements BaseLongColumnValueSelector
  {
    private final List<Long[]> column;
    private final ExtractionFn exFn;

    public TestLongDimensionSelector(Iterable<Long[]> values, ExtractionFn exFn)
    {
      this.exFn = exFn;
      this.column = Lists.newArrayList(
          StreamSupport.stream(values.spliterator(), false).map(input -> Iterators.toArray(
              Iterators.transform(
                  Iterators.forArray(input), new Function<Long, Long>()
                  {
                    @Nullable
                    @Override
                    public Long apply(@Nullable Long input)
                    {
                      return input;
                    }
                  }
              ), Long.class
          )).collect(Collectors.toList())
      );
    }

    @Override
    public IndexedInts getRow()
    {
      return ZeroIndexedInts.instance();
    }

    @Override
    public ValueMatcher makeValueMatcher(@Nullable String value)
    {
      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          return Objects.equals(getValue(), value);
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {

        }
      };
    }

    protected String getValue()
    {
      final int p = this.pos;
      if (exFn == null) {
        return column.get(p)[0].toString();
      } else {
        return exFn.apply(column.get(p)[0]);
      }
    }

    @Override
    public ValueMatcher makeValueMatcher(Predicate<String> predicate)
    {
      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          return predicate.apply(getValue());
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
        }
      };
    }

    @Override
    public int getValueCardinality()
    {
      return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
    }

    @Nullable
    @Override
    public String lookupName(int id)
    {
      return getValue();
    }

    @Override
    public boolean nameLookupPossibleInAdvance()
    {
      return false;
    }

    @Nullable
    @Override
    public IdLookup idLookup()
    {
      return null;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {

    }

    @Override
    public long getLong()
    {
      final int p = this.pos;
      if (exFn == null) {
        return column.get(p)[0];
      } else {
        return Long.valueOf(exFn.apply(column.get(p)[0]));
      }
    }

    @Nullable
    @Override
    public Object getObject()
    {
      return null;
    }

    @Override
    public Class classOfObject()
    {
      return null;
    }
  }


  /**
   * 字符串类型的数据
   */
  private static final List<String[]> values1 = stringDimensionValues(
      "a", "b", "c", "a", "a", null, "b", "b", "b", "b", "a", "a", "d"
  );

  /**
   * 11 个值
   * 基数 6;
   */
  private static final List<Long[]> values2 = longDimensionValues(
      1L,
      2L,
      2L,
      2L,
      3L,
      4L,
      5L,
      5L,
      6L,
      6L,
      6L
  );

  private static final List<Long[]> values3 = longDimensionValues(
      5L,
      5L,
      6L,
      7L,
      7L
  );

  private static List<String[]> stringDimensionValues(Object... values)
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

  private static List<Long[]> longDimensionValues(Object... values)
  {
    return Lists.transform(
        Lists.newArrayList(values), new Function<Object, Long[]>()
        {
          @Nullable
          @Override
          public Long[] apply(@Nullable Object input)
          {
            if (input instanceof Long[]) {
              return (Long[]) input;
            } else {
              return new Long[]{(Long) input};
            }
          }
        }
    );
  }

  private static void aggregate(List<DimensionSelector> selectorList, Aggregator agg)
  {
    agg.aggregate();

    for (DimensionSelector selector : selectorList) {
      AbstractDimensionSelector _selector = (AbstractDimensionSelector) selector;
      _selector.increment();
    }
  }

  private static void bufferAggregate(
      List<DimensionSelector> selectorList,
      BufferAggregator agg,
      ByteBuffer buf,
      int pos
  )
  {
    agg.aggregate(buf, pos);

    for (DimensionSelector selector : selectorList) {
      AbstractDimensionSelector _selector = (AbstractDimensionSelector) selector;
      _selector.increment();
    }
  }

  List<DimensionSelector> selectorList;
  AccurateCardinalityAggregatorFactory strAggregatorFactory;
  AccurateCardinalityAggregatorFactory longAggregatorFactory2;
  AccurateCardinalityAggregatorFactory longAggregatorFactory3;

  final TestStringDimensionSelector dim1;
  final TestLongDimensionSelector dim2;
  final TestLongDimensionSelector dim3;


  List<DimensionSelector> selectorListWithExtraction;
  final TestStringDimensionSelector dim1WithExtraction;
  final TestLongDimensionSelector dim2WithExtraction;

  List<DimensionSelector> selectorListConstantVal;
  final TestStringDimensionSelector dim1ConstantVal;
  final TestLongDimensionSelector dim2ConstantVal;

  final DimensionSpec dimSpec1 = new DefaultDimensionSpec("dim1", "dim1");
  final DimensionSpec dimSpec2 = new DefaultDimensionSpec("dim2", "dim2");
  final DimensionSpec dimSpec3 = new DefaultDimensionSpec("dim3", "dim3");

  public AccurateCardinalityAggregatorTest()
  {
    dim1 = new TestStringDimensionSelector(values1, null);
    dim2 = new TestLongDimensionSelector(values2, null);
    dim3 = new TestLongDimensionSelector(values3, null);

    selectorList = Lists.newArrayList(
        (DimensionSelector) dim1,
        dim2
    );

    strAggregatorFactory = new AccurateCardinalityAggregatorFactory(
        "billy",
        dimSpec1,
        roaringBitmapCollectorFactory
    );

    longAggregatorFactory2 = new AccurateCardinalityAggregatorFactory(
        "UV2",
        dimSpec2,
        roaringBitmapCollectorFactory
    );

    longAggregatorFactory3 = new AccurateCardinalityAggregatorFactory(
        "UV3",
        dimSpec3,
        roaringBitmapCollectorFactory
    );

    String superJsFn1 = "function(str) { return 'super-' + str; }";
    String superJsFn2 = "function(str) { return  str + 10; }";
    ExtractionFn superFn1 = new JavaScriptExtractionFn(superJsFn1, false, JavaScriptConfig.getEnabledInstance());
    ExtractionFn superFn2 = new JavaScriptExtractionFn(superJsFn2, false, JavaScriptConfig.getEnabledInstance());

    dim1WithExtraction = new TestStringDimensionSelector(values1, superFn1);
    dim2WithExtraction = new TestLongDimensionSelector(values2, superFn2);

    selectorListWithExtraction = Lists.newArrayList(
        dim1WithExtraction,
        dim2WithExtraction
    );

    String helloJsFn = "function(str) { return 'hello' }";
    String helloJsFn2 = "function(str) { return 100 }";
    ExtractionFn helloFn = new JavaScriptExtractionFn(helloJsFn, false, JavaScriptConfig.getEnabledInstance());
    ExtractionFn helloFn2 = new JavaScriptExtractionFn(helloJsFn2, false, JavaScriptConfig.getEnabledInstance());
    dim1ConstantVal = new TestStringDimensionSelector(values1, helloFn);
    dim2ConstantVal = new TestLongDimensionSelector(values2, helloFn2);
    selectorListConstantVal = Lists.newArrayList(
        dim1ConstantVal,
        dim2ConstantVal
    );
  }

  @Test
  public void testAggregateLong() throws Exception
  {
    LongAccurateCardinalityAggregator agg = new LongAccurateCardinalityAggregator(
        selectorList.get(1),
        roaringBitmapCollectorFactory,
        true
    );
    for (int i = 0; i < values2.size(); ++i) {
      aggregate(selectorList, agg);
    }
    Assert.assertEquals(6L, longAggregatorFactory2.finalizeComputation(agg.get()));
  }

  @Test
  public void testBufferAggregateLong()
  {
    LongAccurateCardinalityAggregator agg = new LongAccurateCardinalityAggregator(
        selectorList.get(1),
        roaringBitmapCollectorFactory,
        false
    );
    int maxSize = longAggregatorFactory2.getMaxIntermediateSize();

    ByteBuffer buf = ByteBuffer.allocate(maxSize + 64);
    int pos = 10;
    buf.limit(pos + maxSize);

    agg.init(buf, pos);
    for (int i = 0; i < values2.size(); ++i) {
      bufferAggregate(selectorList, agg, buf, pos);
    }
    Assert.assertEquals(6L, longAggregatorFactory2.finalizeComputation(agg.get(buf, pos)));
  }

  @Test
  public void testCombinRows()
  {
    List<DimensionSelector> selector2 = ImmutableList.of(dim2);
    List<DimensionSelector> selector3 = ImmutableList.of(dim3);

    LongAccurateCardinalityAggregator agg2 = new LongAccurateCardinalityAggregator(
        dim2,
        roaringBitmapCollectorFactory,
        true
    );
    LongAccurateCardinalityAggregator agg3 = new LongAccurateCardinalityAggregator(
        dim3,
        roaringBitmapCollectorFactory,
        true
    );

    for (int i = 0; i < values2.size(); ++i) {
      aggregate(selector2, agg2);
    }
    for (int i = 0; i < values3.size(); ++i) {
      aggregate(selector3, agg3);
    }

    Assert.assertEquals(6L, longAggregatorFactory2.finalizeComputation(agg2.get()));
    Assert.assertEquals(3L, longAggregatorFactory3.finalizeComputation(agg3.get()));

    Assert.assertEquals(
        7L,
        longAggregatorFactory2.finalizeComputation(
            longAggregatorFactory2.combine(
                agg2.get(),
                agg3.get()
            )
        )
    );
  }

  @Test
  public void testAggregateLongWithExtraction()
  {
    LongAccurateCardinalityAggregator agg = new LongAccurateCardinalityAggregator(
        selectorListWithExtraction.get(1),
        roaringBitmapCollectorFactory,
        true
    );
    for (int i = 0; i < values2.size(); ++i) {
      aggregate(selectorListWithExtraction, agg);
    }
    Assert.assertEquals(6L, longAggregatorFactory2.finalizeComputation(agg.get()));

    LongAccurateCardinalityAggregator agg2 = new LongAccurateCardinalityAggregator(
        selectorListConstantVal.get(1),
        roaringBitmapCollectorFactory,
        true
    );
    for (int i = 0; i < values2.size(); ++i) {
      aggregate(selectorListConstantVal, agg2);
    }
    Assert.assertEquals(1L, longAggregatorFactory2.finalizeComputation(agg2.get()));
  }

  @Test
  public void testBufferAggregateLongWithExtraction()
  {
    LongAccurateCardinalityAggregator agg = new LongAccurateCardinalityAggregator(
        selectorListWithExtraction.get(1),
        roaringBitmapCollectorFactory,
        false
    );

    int maxSize = longAggregatorFactory2.getMaxIntermediateSize();

    ByteBuffer buf = ByteBuffer.allocate(maxSize + 64);
    int pos = 10;
    buf.limit(pos + maxSize);

    agg.init(buf, pos);
    for (int i = 0; i < values2.size(); ++i) {
      bufferAggregate(selectorListWithExtraction, agg, buf, pos);
    }

    Assert.assertEquals(6L, longAggregatorFactory2.finalizeComputation(agg.get(buf, pos)));

    LongAccurateCardinalityAggregator agg2 = new LongAccurateCardinalityAggregator(
        selectorListConstantVal.get(1),
        roaringBitmapCollectorFactory,
        false
    );
    pos = buf.limit();
    buf.limit(buf.limit() + maxSize);
    agg2.init(buf, pos);
    for (int i = 0; i < values2.size(); ++i) {
      bufferAggregate(selectorListConstantVal, agg2, buf, pos);
    }

    Assert.assertEquals(1L, longAggregatorFactory2.finalizeComputation(agg2.get(buf, pos)));
  }

  @Test
  public void testSerde() throws IOException
  {
    AccurateCardinalityAggregatorFactory expectedFactory = new AccurateCardinalityAggregatorFactory(
        "UV",
        "UV",
        roaringBitmapCollectorFactory
    );

    ObjectMapper objectMapper = new DefaultObjectMapper();
    AccurateCardinalityModule acm = new AccurateCardinalityModule();
    acm.getJacksonModules().forEach(objectMapper::registerModule);

    String jsonStr = "{\"type\":\"accurateCardinality\",\"name\":\"UV\",\"field\":\"UV\"}";

    AccurateCardinalityAggregatorFactory factory;
    factory = objectMapper.readValue(jsonStr, AccurateCardinalityAggregatorFactory.class);
    Assert.assertEquals("", expectedFactory.getLongBitmapCollectorFactory(), factory.getLongBitmapCollectorFactory());
    Assert.assertEquals("", expectedFactory.getName(), factory.getName());
  }

}
