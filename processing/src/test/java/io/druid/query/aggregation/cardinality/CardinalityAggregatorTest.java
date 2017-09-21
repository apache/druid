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

package io.druid.query.aggregation.cardinality;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.js.JavaScriptConfig;
import io.druid.query.ColumnSelectorPlus;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.cardinality.types.CardinalityAggregatorColumnSelectorStrategy;
import io.druid.query.aggregation.cardinality.types.StringCardinalityAggregatorColumnSelectorStrategy;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.dimension.RegexFilteredDimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.JavaScriptExtractionFn;
import io.druid.query.extraction.RegexDimExtractionFn;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DimensionSelectorUtils;
import io.druid.segment.IdLookup;
import io.druid.segment.data.IndexedInts;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class CardinalityAggregatorTest
{
  public static class TestDimensionSelector implements DimensionSelector
  {
    private final List<Integer[]> column;
    private final Map<String, Integer> ids;
    private final Map<Integer, String> lookup;
    private final ExtractionFn exFn;

    private int pos = 0;

    public TestDimensionSelector(Iterable<String[]> values, ExtractionFn exFn)
    {
      this.lookup = Maps.newHashMap();
      this.ids = Maps.newHashMap();
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
          Iterables.transform(
              values, new Function<String[], Integer[]>()
              {
                @Nullable
                @Override
                public Integer[] apply(@Nullable String[] input)
                {
                  return Iterators.toArray(
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
                  );
                }
              }
          )
      );
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
        public void close() throws IOException
        {
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          // Don't care about runtime shape in tests
        }
      };
    }

    @Override
    public ValueMatcher makeValueMatcher(String value)
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

    @Override
    public String lookupName(int i)
    {
      String val = lookup.get(i);
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
  }

  /*
    values1: 4 distinct rows
    values1: 4 distinct values
    values2: 8 distinct rows
    values2: 7 distinct values
    groupBy(values1, values2): 9 distinct rows
    groupBy(values1, values2): 7 distinct values
    combine(values1, values2): 8 distinct rows
    combine(values1, values2): 7 distinct values
   */
  private static final List<String[]> values1 = dimensionValues(
      "a", "b", "c", "a", "a", null, "b", "b", "b", "b", "a", "a"
  );
  private static final List<String[]> values2 = dimensionValues(
      "a",
      "b",
      "c",
      "x",
      "a",
      "e",
      "b",
      new String[]{null, "x"},
      new String[]{"x", null},
      new String[]{"y", "x"},
      new String[]{"x", "y"},
      new String[]{"x", "y", "a"}
  );

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

  private static void aggregate(List<DimensionSelector> selectorList, Aggregator agg)
  {
    agg.aggregate();

    for (DimensionSelector selector : selectorList) {
      ((TestDimensionSelector) selector).increment();
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
      ((TestDimensionSelector) selector).increment();
    }
  }

  List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> dimInfoList;
  List<DimensionSelector> selectorList;
  CardinalityAggregatorFactory rowAggregatorFactory;
  CardinalityAggregatorFactory rowAggregatorFactoryRounded;
  CardinalityAggregatorFactory valueAggregatorFactory;
  final TestDimensionSelector dim1;
  final TestDimensionSelector dim2;

  List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> dimInfoListWithExtraction;
  List<DimensionSelector> selectorListWithExtraction;
  final TestDimensionSelector dim1WithExtraction;
  final TestDimensionSelector dim2WithExtraction;

  List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> dimInfoListConstantVal;
  List<DimensionSelector> selectorListConstantVal;
  final TestDimensionSelector dim1ConstantVal;
  final TestDimensionSelector dim2ConstantVal;

  final DimensionSpec dimSpec1 = new DefaultDimensionSpec("dim1", "dim1");
  final DimensionSpec dimSpec2 = new DefaultDimensionSpec("dim2", "dim2");

  public CardinalityAggregatorTest()
  {
    dim1 = new TestDimensionSelector(values1, null);
    dim2 = new TestDimensionSelector(values2, null);

    dimInfoList = Lists.newArrayList(
        new ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>(
            dimSpec1.getDimension(),
            dimSpec1.getOutputName(),
            new StringCardinalityAggregatorColumnSelectorStrategy(), dim1
        ),
        new ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>(
            dimSpec2.getDimension(),
            dimSpec2.getOutputName(),
            new StringCardinalityAggregatorColumnSelectorStrategy(), dim2
        )
    );

    selectorList = Lists.newArrayList(
        (DimensionSelector) dim1,
        dim2
    );

    rowAggregatorFactory = new CardinalityAggregatorFactory(
        "billy",
        Lists.<DimensionSpec>newArrayList(
            dimSpec1,
            dimSpec2
        ),
        true
    );

    rowAggregatorFactoryRounded = new CardinalityAggregatorFactory(
        "billy",
        null,
        Lists.<DimensionSpec>newArrayList(
            dimSpec1,
            dimSpec2
        ),
        true,
        true
    );

    valueAggregatorFactory = new CardinalityAggregatorFactory(
        "billy",
        Lists.<DimensionSpec>newArrayList(
            dimSpec1,
            dimSpec2
        ),
        false
    );


    String superJsFn = "function(str) { return 'super-' + str; }";
    ExtractionFn superFn = new JavaScriptExtractionFn(superJsFn, false, JavaScriptConfig.getEnabledInstance());
    dim1WithExtraction = new TestDimensionSelector(values1, superFn);
    dim2WithExtraction = new TestDimensionSelector(values2, superFn);
    selectorListWithExtraction = Lists.newArrayList(
        (DimensionSelector) dim1WithExtraction,
        dim2WithExtraction
    );
    dimInfoListWithExtraction = Lists.newArrayList(
        new ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>(
            dimSpec1.getDimension(),
            dimSpec1.getOutputName(),
            new StringCardinalityAggregatorColumnSelectorStrategy(), dim1WithExtraction
        ),
        new ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>(
            dimSpec1.getDimension(),
            dimSpec1.getOutputName(),
            new StringCardinalityAggregatorColumnSelectorStrategy(), dim2WithExtraction
        )
    );

    String helloJsFn = "function(str) { return 'hello' }";
    ExtractionFn helloFn = new JavaScriptExtractionFn(helloJsFn, false, JavaScriptConfig.getEnabledInstance());
    dim1ConstantVal = new TestDimensionSelector(values1, helloFn);
    dim2ConstantVal = new TestDimensionSelector(values2, helloFn);
    selectorListConstantVal = Lists.newArrayList(
        (DimensionSelector) dim1ConstantVal,
        dim2ConstantVal
    );
    dimInfoListConstantVal = Lists.newArrayList(
        new ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>(
            dimSpec1.getDimension(),
            dimSpec1.getOutputName(),
            new StringCardinalityAggregatorColumnSelectorStrategy(), dim1ConstantVal
        ),
        new ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>(
            dimSpec1.getDimension(),
            dimSpec1.getOutputName(),
            new StringCardinalityAggregatorColumnSelectorStrategy(), dim2ConstantVal
        )
    );

  }

  @Test
  public void testAggregateRows() throws Exception
  {
    CardinalityAggregator agg = new CardinalityAggregator(
        "billy",
        dimInfoList,
        true
    );


    for (int i = 0; i < values1.size(); ++i) {
      aggregate(selectorList, agg);
    }
    Assert.assertEquals(9.0, (Double) rowAggregatorFactory.finalizeComputation(agg.get()), 0.05);
    Assert.assertEquals(9L, rowAggregatorFactoryRounded.finalizeComputation(agg.get()));
  }

  @Test
  public void testAggregateValues() throws Exception
  {
    CardinalityAggregator agg = new CardinalityAggregator(
        "billy",
        dimInfoList,
        false
    );

    for (int i = 0; i < values1.size(); ++i) {
      aggregate(selectorList, agg);
    }
    Assert.assertEquals(7.0, (Double) valueAggregatorFactory.finalizeComputation(agg.get()), 0.05);
    Assert.assertEquals(7L, rowAggregatorFactoryRounded.finalizeComputation(agg.get()));
  }

  @Test
  public void testBufferAggregateRows() throws Exception
  {
    CardinalityBufferAggregator agg = new CardinalityBufferAggregator(
        dimInfoList.toArray(new ColumnSelectorPlus[] {}),
        true
    );

    int maxSize = rowAggregatorFactory.getMaxIntermediateSize();
    ByteBuffer buf = ByteBuffer.allocate(maxSize + 64);
    int pos = 10;
    buf.limit(pos + maxSize);

    agg.init(buf, pos);

    for (int i = 0; i < values1.size(); ++i) {
      bufferAggregate(selectorList, agg, buf, pos);
    }
    Assert.assertEquals(9.0, (Double) rowAggregatorFactory.finalizeComputation(agg.get(buf, pos)), 0.05);
    Assert.assertEquals(9L, rowAggregatorFactoryRounded.finalizeComputation(agg.get(buf, pos)));
  }

  @Test
  public void testBufferAggregateValues() throws Exception
  {
    CardinalityBufferAggregator agg = new CardinalityBufferAggregator(
        dimInfoList.toArray(new ColumnSelectorPlus[] {}),
        false
    );

    int maxSize = valueAggregatorFactory.getMaxIntermediateSize();
    ByteBuffer buf = ByteBuffer.allocate(maxSize + 64);
    int pos = 10;
    buf.limit(pos + maxSize);

    agg.init(buf, pos);

    for (int i = 0; i < values1.size(); ++i) {
      bufferAggregate(selectorList, agg, buf, pos);
    }
    Assert.assertEquals(7.0, (Double) valueAggregatorFactory.finalizeComputation(agg.get(buf, pos)), 0.05);
    Assert.assertEquals(7L, rowAggregatorFactoryRounded.finalizeComputation(agg.get(buf, pos)));
  }

  @Test
  public void testCombineRows()
  {
    List<DimensionSelector> selector1 = Lists.newArrayList((DimensionSelector) dim1);
    List<DimensionSelector> selector2 = Lists.newArrayList((DimensionSelector) dim2);
    List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> dimInfo1 = Lists.newArrayList(
        new ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>(
            dimSpec1.getDimension(),
            dimSpec1.getOutputName(),
            new StringCardinalityAggregatorColumnSelectorStrategy(), dim1
        )
    );
    List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> dimInfo2 = Lists.newArrayList(
        new ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>(
            dimSpec1.getDimension(),
            dimSpec1.getOutputName(),
            new StringCardinalityAggregatorColumnSelectorStrategy(), dim2
        )
    );

    CardinalityAggregator agg1 = new CardinalityAggregator("billy", dimInfo1, true);
    CardinalityAggregator agg2 = new CardinalityAggregator("billy", dimInfo2, true);

    for (int i = 0; i < values1.size(); ++i) {
      aggregate(selector1, agg1);
    }
    for (int i = 0; i < values2.size(); ++i) {
      aggregate(selector2, agg2);
    }

    Assert.assertEquals(4.0, (Double) rowAggregatorFactory.finalizeComputation(agg1.get()), 0.05);
    Assert.assertEquals(8.0, (Double) rowAggregatorFactory.finalizeComputation(agg2.get()), 0.05);

    Assert.assertEquals(
        9.0,
        (Double) rowAggregatorFactory.finalizeComputation(
            rowAggregatorFactory.combine(
                agg1.get(),
                agg2.get()
            )
        ),
        0.05
    );
  }

  @Test
  public void testCombineValues()
  {
    List<DimensionSelector> selector1 = Lists.newArrayList((DimensionSelector) dim1);
    List<DimensionSelector> selector2 = Lists.newArrayList((DimensionSelector) dim2);

    List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> dimInfo1 = Lists.newArrayList(
        new ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>(
            dimSpec1.getDimension(),
            dimSpec1.getOutputName(),
            new StringCardinalityAggregatorColumnSelectorStrategy(), dim1
        )
    );
    List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> dimInfo2 = Lists.newArrayList(
        new ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>(
            dimSpec1.getDimension(),
            dimSpec1.getOutputName(),
            new StringCardinalityAggregatorColumnSelectorStrategy(), dim2
        )
    );

    CardinalityAggregator agg1 = new CardinalityAggregator("billy", dimInfo1, false);
    CardinalityAggregator agg2 = new CardinalityAggregator("billy", dimInfo2, false);

    for (int i = 0; i < values1.size(); ++i) {
      aggregate(selector1, agg1);
    }
    for (int i = 0; i < values2.size(); ++i) {
      aggregate(selector2, agg2);
    }

    Assert.assertEquals(4.0, (Double) valueAggregatorFactory.finalizeComputation(agg1.get()), 0.05);
    Assert.assertEquals(7.0, (Double) valueAggregatorFactory.finalizeComputation(agg2.get()), 0.05);

    Assert.assertEquals(
        7.0,
        (Double) rowAggregatorFactory.finalizeComputation(
            rowAggregatorFactory.combine(
                agg1.get(),
                agg2.get()
            )
        ),
        0.05
    );
  }

  @Test
  public void testAggregateRowsWithExtraction() throws Exception
  {
    CardinalityAggregator agg = new CardinalityAggregator(
        "billy",
        dimInfoListWithExtraction,
        true
    );
    for (int i = 0; i < values1.size(); ++i) {
      aggregate(selectorListWithExtraction, agg);
    }
    Assert.assertEquals(9.0, (Double) rowAggregatorFactory.finalizeComputation(agg.get()), 0.05);

    CardinalityAggregator agg2 = new CardinalityAggregator(
        "billy",
        dimInfoListConstantVal,
        true
    );
    for (int i = 0; i < values1.size(); ++i) {
      aggregate(selectorListConstantVal, agg2);
    }
    Assert.assertEquals(3.0, (Double) rowAggregatorFactory.finalizeComputation(agg2.get()), 0.05);
  }

  @Test
  public void testAggregateValuesWithExtraction() throws Exception
  {
    CardinalityAggregator agg = new CardinalityAggregator(
        "billy",
        dimInfoListWithExtraction,
        false
    );
    for (int i = 0; i < values1.size(); ++i) {
      aggregate(selectorListWithExtraction, agg);
    }
    Assert.assertEquals(7.0, (Double) valueAggregatorFactory.finalizeComputation(agg.get()), 0.05);

    CardinalityAggregator agg2 = new CardinalityAggregator(
        "billy",
        dimInfoListConstantVal,
        false
    );
    for (int i = 0; i < values1.size(); ++i) {
      aggregate(selectorListConstantVal, agg2);
    }
    Assert.assertEquals(1.0, (Double) valueAggregatorFactory.finalizeComputation(agg2.get()), 0.05);
  }

  @Test
  public void testSerde() throws Exception
  {
    CardinalityAggregatorFactory factory = new CardinalityAggregatorFactory(
        "billy",
        null,
        ImmutableList.<DimensionSpec>of(
            new DefaultDimensionSpec("b", "b"),
            new DefaultDimensionSpec("a", "a"),
            new DefaultDimensionSpec("c", "c")
        ),
        true,
        true
    );
    ObjectMapper objectMapper = new DefaultObjectMapper();
    Assert.assertEquals(
        factory,
        objectMapper.readValue(objectMapper.writeValueAsString(factory), AggregatorFactory.class)
    );

    String fieldNamesOnly = "{"
                            + "\"type\":\"cardinality\","
                            + "\"name\":\"billy\","
                            + "\"fields\":[\"b\",\"a\",\"c\"],"
                            + "\"byRow\":true,"
                            + "\"round\":true"
                            + "}";
    Assert.assertEquals(
        factory,
        objectMapper.readValue(fieldNamesOnly, AggregatorFactory.class)
    );

    CardinalityAggregatorFactory factory2 = new CardinalityAggregatorFactory(
        "billy",
        ImmutableList.<DimensionSpec>of(
            new ExtractionDimensionSpec("b", "b", new RegexDimExtractionFn(".*", false, null)),
            new RegexFilteredDimensionSpec(new DefaultDimensionSpec("a", "a"), ".*"),
            new DefaultDimensionSpec("c", "c")
        ),
        true
    );

    Assert.assertEquals(
        factory2,
        objectMapper.readValue(objectMapper.writeValueAsString(factory2), AggregatorFactory.class)
    );
  }
}
