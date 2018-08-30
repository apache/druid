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

package org.apache.druid.query.aggregation;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.JavaScriptExtractionFn;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.JavaScriptDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.RegexDimFilter;
import org.apache.druid.query.filter.SearchQueryDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.search.ContainsSearchQuerySpec;
import org.apache.druid.segment.AbstractDimensionSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.SingleIndexedInt;
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

    FilteredAggregator agg = (FilteredAggregator) factory.factorize(makeColumnSelector(selector));

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

        if ("dim".equals(dimensionName)) {
          return dimensionSpec.decorate(
              new AbstractDimensionSelector()
              {
                @Override
                public IndexedInts getRow()
                {
                  SingleIndexedInt row = new SingleIndexedInt();
                  if (selector.getIndex() % 3 == 2) {
                    row.setValue(1);
                  } else {
                    row.setValue(0);
                  }
                  return row;
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
                public Class classOfObject()
                {
                  return Object.class;
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
      public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
      {
        if ("value".equals(columnName)) {
          return selector;
        } else {
          throw new UnsupportedOperationException();
        }
      }

      @Override
      public ColumnCapabilities getColumnCapabilities(String columnName)
      {
        ColumnCapabilitiesImpl caps;
        if ("value".equals(columnName)) {
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

  private void assertValues(FilteredAggregator agg, TestFloatColumnSelector selector, double... expectedVals)
  {
    Assert.assertEquals(NullHandling.defaultDoubleValue(), agg.get());
    Assert.assertEquals(NullHandling.defaultDoubleValue(), agg.get());
    Assert.assertEquals(NullHandling.defaultDoubleValue(), agg.get());
    for (double expectedVal : expectedVals) {
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
        new OrDimFilter(Lists.newArrayList(new SelectorDimFilter("dim", "a", null), new SelectorDimFilter("dim", "b", null)))
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
        new AndDimFilter(Lists.newArrayList(new NotDimFilter(new SelectorDimFilter("dim", "b", null)), new SelectorDimFilter("dim", "a", null))));

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
