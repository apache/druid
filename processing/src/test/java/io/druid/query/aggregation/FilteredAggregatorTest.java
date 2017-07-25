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

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import io.druid.js.JavaScriptConfig;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.JavaScriptExtractionFn;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.JavaScriptDimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.RegexDimFilter;
import io.druid.query.filter.SearchQueryDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.query.ordering.StringComparators;
import io.druid.query.search.search.ContainsSearchQuerySpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DimensionSelectorUtils;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.IdLookup;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.ArrayBasedIndexedInts;
import io.druid.segment.data.IndexedInts;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;

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
        new SelectorDimFilter("dim", "a", null)
    );

    FilteredAggregator agg = (FilteredAggregator) factory.factorize(
     makeColumnSelector(selector)
    );

    double expectedFirst = new Float(values[0]).doubleValue();
    double expectedSecond = new Float(values[1]).doubleValue() + expectedFirst;
    double expectedThird = expectedSecond;

    assertValues(agg, selector, expectedFirst, expectedSecond, expectedThird);
  }

  private ColumnSelectorFactory makeColumnSelector(final TestFloatColumnSelector selector)
  {

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
                    return ArrayBasedIndexedInts.of(new int[]{1});
                  } else {
                    return ArrayBasedIndexedInts.of(new int[]{0});
                  }
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
                  };
                }

                @Override
                public void inspectRuntimeShape(RuntimeShapeInspector inspector)
                {
                  // Don't care about runtime shape in tests
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
      public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
      {
        if (columnName.equals("value")) {
          return new DoubleColumnSelector()
          {
            @Override
            public double get()
            {
              return (double) selector.get();
            }

            @Override
            public void inspectRuntimeShape(RuntimeShapeInspector inspector)
            {

            }
          };
        } else {
          throw new UnsupportedOperationException();
        }
      }

      @Override
      public ObjectColumnSelector makeObjectColumnSelector(String columnName)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public ColumnCapabilities getColumnCapabilities(String columnName)
      {
        ColumnCapabilitiesImpl caps;
        if (columnName.equals("value")) {
          caps = new ColumnCapabilitiesImpl();
          caps.setType(ValueType.FLOAT);
          caps.setDictionaryEncoded(false);
          caps.setHasBitmapIndexes(false);
        } else {
          caps = new ColumnCapabilitiesImpl();
          caps.setType(ValueType.STRING);
          caps.setDictionaryEncoded(true);
          caps.setHasBitmapIndexes(true);
        }
        return caps;
      }
    };
  }

  private void assertValues(FilteredAggregator agg,TestFloatColumnSelector selector, double... expectedVals)
  {
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
        new NotDimFilter(new SelectorDimFilter("dim", "b", null))
    );

    validateFilteredAggs(factory, values, selector);
  }

  @Test
  public void testAggregateWithOrFilter()
  {
    final float[] values = {0.15f, 0.27f, 0.14f};
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

    FilteredAggregatorFactory factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new OrDimFilter(Lists.<DimFilter>newArrayList(new SelectorDimFilter("dim", "a", null), new SelectorDimFilter("dim", "b", null)))
    );

    FilteredAggregator agg = (FilteredAggregator) factory.factorize(
        makeColumnSelector(selector)
    );

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
        new AndDimFilter(Lists.<DimFilter>newArrayList(new NotDimFilter(new SelectorDimFilter("dim", "b", null)), new SelectorDimFilter("dim", "a", null))));

    validateFilteredAggs(factory, values, selector);
  }

  @Test
  public void testAggregateWithPredicateFilters()
  {
    final float[] values = {0.15f, 0.27f};
    TestFloatColumnSelector selector;
    FilteredAggregatorFactory factory;

    factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new BoundDimFilter("dim", "a", "a", false, false, true, null, StringComparators.ALPHANUMERIC)
    );
    selector = new TestFloatColumnSelector(values);
    validateFilteredAggs(factory, values, selector);

    factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new RegexDimFilter("dim", "a", null)
    );
    selector = new TestFloatColumnSelector(values);
    validateFilteredAggs(factory, values, selector);

    factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new SearchQueryDimFilter("dim", new ContainsSearchQuerySpec("a", true), null)
    );
    selector = new TestFloatColumnSelector(values);
    validateFilteredAggs(factory, values, selector);

    String jsFn = "function(x) { return(x === 'a') }";
    factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new JavaScriptDimFilter("dim", jsFn, null, JavaScriptConfig.getEnabledInstance())
    );
    selector = new TestFloatColumnSelector(values);
    validateFilteredAggs(factory, values, selector);
  }

  @Test
  public void testAggregateWithExtractionFns()
  {
    final float[] values = {0.15f, 0.27f};
    TestFloatColumnSelector selector;
    FilteredAggregatorFactory factory;

    String extractionJsFn = "function(str) { return str + 'AARDVARK'; }";
    ExtractionFn extractionFn = new JavaScriptExtractionFn(extractionJsFn, false, JavaScriptConfig.getEnabledInstance());

    factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new SelectorDimFilter("dim", "aAARDVARK", extractionFn)
    );
    selector = new TestFloatColumnSelector(values);
    validateFilteredAggs(factory, values, selector);

    factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new InDimFilter("dim", Arrays.asList("NOT-aAARDVARK", "FOOBAR", "aAARDVARK"), extractionFn)
    );
    selector = new TestFloatColumnSelector(values);
    validateFilteredAggs(factory, values, selector);

    factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new BoundDimFilter("dim", "aAARDVARK", "aAARDVARK", false, false, true, extractionFn,
                           StringComparators.ALPHANUMERIC
        )
    );
    selector = new TestFloatColumnSelector(values);
    validateFilteredAggs(factory, values, selector);

    factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new RegexDimFilter("dim", "aAARDVARK", extractionFn)
    );
    selector = new TestFloatColumnSelector(values);
    validateFilteredAggs(factory, values, selector);

    factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new SearchQueryDimFilter("dim", new ContainsSearchQuerySpec("aAARDVARK", true), extractionFn)
    );
    selector = new TestFloatColumnSelector(values);
    validateFilteredAggs(factory, values, selector);

    String jsFn = "function(x) { return(x === 'aAARDVARK') }";
    factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new JavaScriptDimFilter("dim", jsFn, extractionFn, JavaScriptConfig.getEnabledInstance())
    );
    selector = new TestFloatColumnSelector(values);
    validateFilteredAggs(factory, values, selector);
  }

  private void validateFilteredAggs(
      FilteredAggregatorFactory factory,
      float[] values,
      TestFloatColumnSelector selector
  )
  {
    FilteredAggregator agg = (FilteredAggregator) factory.factorize(
        makeColumnSelector(selector)
    );

    double expectedFirst = new Float(values[0]).doubleValue();
    double expectedSecond = new Float(values[1]).doubleValue() + expectedFirst;
    double expectedThird = expectedSecond;

    assertValues(agg, selector, expectedFirst, expectedSecond, expectedThird);
  }
}
