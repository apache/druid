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

package io.druid.query.aggregation;

import com.google.common.collect.Lists;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.data.ArrayBasedIndexedInts;
import io.druid.segment.data.IndexedInts;
import org.junit.Assert;
import org.junit.Test;

public class FilteredAggregatorTest
{
  private void aggregate(TestFloatColumnSelector selector, FilteredAggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  @Test
  public void testAggregate()
  {
    final float[] values = {0.15f, 0.27f};
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

    FilteredAggregatorFactory factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new SelectorDimFilter("dim", "a")
    );

    FilteredAggregator agg = (FilteredAggregator) factory.factorize(
     makeColumnSelector(selector)
    );

    Assert.assertEquals("billy", agg.getName());

    double expectedFirst = new Float(values[0]).doubleValue();
    double expectedSecond = new Float(values[1]).doubleValue() + expectedFirst;
    double expectedThird = expectedSecond;

    assertValues(agg, selector, expectedFirst, expectedSecond, expectedThird);
  }

  private ColumnSelectorFactory makeColumnSelector(final TestFloatColumnSelector selector){

    return new ColumnSelectorFactory()
    {
      @Override
      public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
      {
        final String dimensionName = dimensionSpec.getDimension();
        final ExtractionFn extractionFn = dimensionSpec.getExtractionFn();

        if (dimensionName.equals("dim")) {
          return dimensionSpec.decorate(
              new DimensionSelector()
              {
                @Override
                public IndexedInts getRow()
                {
                  if (selector.getIndex() % 3 == 2) {
                    return new ArrayBasedIndexedInts(new int[]{1});
                  } else {
                    return new ArrayBasedIndexedInts(new int[]{0});
                  }
                }

                @Override
                public int getValueCardinality()
                {
                  return 2;
                }

                @Override
                public String lookupName(int id)
                {
                  switch (id) {
                    case 0:
                      return "a";
                    case 1:
                      return "b";
                    default:
                      throw new IllegalArgumentException();
                  }
                }

                @Override
                public int lookupId(String name)
                {
                  switch (name) {
                    case "a":
                      return 0;
                    case "b":
                      return 1;
                    default:
                      throw new IllegalArgumentException();
                  }
                }
              }
          );
        } else {
          throw new UnsupportedOperationException();
        }
      }

      @Override
      public LongColumnSelector makeLongColumnSelector(String columnName)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public FloatColumnSelector makeFloatColumnSelector(String columnName)
      {
        if (columnName.equals("value")) {
          return selector;
        } else {
          throw new UnsupportedOperationException();
        }
      }

      @Override
      public ObjectColumnSelector makeObjectColumnSelector(String columnName)
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  private void assertValues(FilteredAggregator agg,TestFloatColumnSelector selector, double... expectedVals){
    Assert.assertEquals(0.0d, agg.get());
    Assert.assertEquals(0.0d, agg.get());
    Assert.assertEquals(0.0d, agg.get());
    for(double expectedVal : expectedVals){
      aggregate(selector, agg);
      Assert.assertEquals(expectedVal, agg.get());
      Assert.assertEquals(expectedVal, agg.get());
      Assert.assertEquals(expectedVal, agg.get());
    }
  }

  @Test
  public void testAggregateWithNotFilter()
  {
    final float[] values = {0.15f, 0.27f};
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

    FilteredAggregatorFactory factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new NotDimFilter(new SelectorDimFilter("dim", "b"))
    );

    FilteredAggregator agg = (FilteredAggregator) factory.factorize(
        makeColumnSelector(selector)
    );

    Assert.assertEquals("billy", agg.getName());

    double expectedFirst = new Float(values[0]).doubleValue();
    double expectedSecond = new Float(values[1]).doubleValue() + expectedFirst;
    double expectedThird = expectedSecond;
    assertValues(agg, selector, expectedFirst, expectedSecond, expectedThird);
  }

  @Test
  public void testAggregateWithOrFilter()
  {
    final float[] values = {0.15f, 0.27f, 0.14f};
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

    FilteredAggregatorFactory factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new OrDimFilter(Lists.<DimFilter>newArrayList(new SelectorDimFilter("dim", "a"), new SelectorDimFilter("dim", "b")))
    );

    FilteredAggregator agg = (FilteredAggregator) factory.factorize(
        makeColumnSelector(selector)
    );

    Assert.assertEquals("billy", agg.getName());

    double expectedFirst = new Float(values[0]).doubleValue();
    double expectedSecond = new Float(values[1]).doubleValue() + expectedFirst;
    double expectedThird = expectedSecond + new Float(values[2]).doubleValue();
    assertValues(agg, selector, expectedFirst, expectedSecond, expectedThird);
  }

  @Test
  public void testAggregateWithAndFilter()
  {
    final float[] values = {0.15f, 0.27f};
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

    FilteredAggregatorFactory factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new AndDimFilter(Lists.<DimFilter>newArrayList(new NotDimFilter(new SelectorDimFilter("dim", "b")), new SelectorDimFilter("dim", "a"))));

    FilteredAggregator agg = (FilteredAggregator) factory.factorize(
        makeColumnSelector(selector)
    );

    Assert.assertEquals("billy", agg.getName());

    double expectedFirst = new Float(values[0]).doubleValue();
    double expectedSecond = new Float(values[1]).doubleValue() + expectedFirst;
    double expectedThird = expectedSecond;
    assertValues(agg, selector, expectedFirst, expectedSecond, expectedThird);
  }

}
