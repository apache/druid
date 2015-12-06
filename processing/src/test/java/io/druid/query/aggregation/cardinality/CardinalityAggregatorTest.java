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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CardinalityAggregatorTest
{
  public static class TestDimensionSelector implements DimensionSelector
  {
    private final List<Integer[]> column;
    private final Map<String, Integer> ids;
    private final Map<Integer, String> lookup;

    private int pos = 0;

    public TestDimensionSelector(Iterable<String[]> values)
    {
      this.lookup = Maps.newHashMap();
      this.ids = Maps.newHashMap();

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
        public Iterator<Integer> iterator()
        {
          return Iterators.forArray(column.get(p));
        }

        @Override
        public void fill(int index, int[] toFill)
        {
          throw new UnsupportedOperationException("fill not supported");
        }

        @Override
        public void close() throws IOException
        {

        }
      };
    }

    @Override
    public int getValueCardinality()
    {
      return 1;
    }

    @Override
    public String lookupName(int i)
    {
      return lookup.get(i);
    }

    @Override
    public int lookupId(String s)
    {
      return ids.get(s);
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

  List<DimensionSelector> selectorList;
  CardinalityAggregatorFactory rowAggregatorFactory;
  CardinalityAggregatorFactory valueAggregatorFactory;
  final TestDimensionSelector dim1;
  final TestDimensionSelector dim2;

  public CardinalityAggregatorTest()
  {
    dim1 = new TestDimensionSelector(values1);
    dim2 = new TestDimensionSelector(values2);

    selectorList = Lists.newArrayList(
        (DimensionSelector) dim1,
        dim2
    );

    rowAggregatorFactory = new CardinalityAggregatorFactory(
        "billy",
        Lists.newArrayList("dim1", "dim2"),
        true
    );

    valueAggregatorFactory = new CardinalityAggregatorFactory(
        "billy",
        Lists.newArrayList("dim1", "dim2"),
        true
    );
  }

  @Test
  public void testAggregateRows() throws Exception
  {
    CardinalityAggregator agg = new CardinalityAggregator(
        "billy",
        selectorList,
        true
    );


    for (int i = 0; i < values1.size(); ++i) {
      aggregate(selectorList, agg);
    }
    Assert.assertEquals(9.0, (Double) rowAggregatorFactory.finalizeComputation(agg.get()), 0.05);
  }

  @Test
  public void testAggregateValues() throws Exception
  {
    CardinalityAggregator agg = new CardinalityAggregator(
        "billy",
        selectorList,
        false
    );

    for (int i = 0; i < values1.size(); ++i) {
      aggregate(selectorList, agg);
    }
    Assert.assertEquals(7.0, (Double) valueAggregatorFactory.finalizeComputation(agg.get()), 0.05);
  }

  @Test
  public void testBufferAggregateRows() throws Exception
  {
    CardinalityBufferAggregator agg = new CardinalityBufferAggregator(
        selectorList,
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
  }

  @Test
  public void testBufferAggregateValues() throws Exception
  {
    CardinalityBufferAggregator agg = new CardinalityBufferAggregator(
        selectorList,
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
  }

  @Test
  public void testCombineRows()
  {
    List<DimensionSelector> selector1 = Lists.newArrayList((DimensionSelector) dim1);
    List<DimensionSelector> selector2 = Lists.newArrayList((DimensionSelector) dim2);

    CardinalityAggregator agg1 = new CardinalityAggregator("billy", selector1, true);
    CardinalityAggregator agg2 = new CardinalityAggregator("billy", selector2, true);

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

    CardinalityAggregator agg1 = new CardinalityAggregator("billy", selector1, false);
    CardinalityAggregator agg2 = new CardinalityAggregator("billy", selector2, false);

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
  public void testSerde() throws Exception
  {
    CardinalityAggregatorFactory factory = new CardinalityAggregatorFactory("billy", ImmutableList.of("b", "a", "c"), true);
    ObjectMapper objectMapper = new DefaultObjectMapper();
    Assert.assertEquals(
        factory,
        objectMapper.readValue(objectMapper.writeValueAsString(factory), AggregatorFactory.class)
    );
  }
}
