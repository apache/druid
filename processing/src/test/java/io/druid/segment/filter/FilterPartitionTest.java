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

package io.druid.segment.filter;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.js.JavaScriptConfig;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.JavaScriptExtractionFn;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DruidLongPredicate;
import io.druid.query.filter.DruidPredicateFactory;
import io.druid.query.filter.Filter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.IndexBuilder;
import io.druid.segment.StorageAdapter;
import io.druid.segment.data.IndexedInts;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@RunWith(Parameterized.class)
public class FilterPartitionTest extends BaseFilterTest
{
  private class NoBitmapSelectorFilter extends SelectorFilter
  {
    public NoBitmapSelectorFilter(
        String dimension,
        String value
    )
    {
      super(dimension, value);
    }

    @Override
    public boolean supportsBitmapIndex(BitmapIndexSelector selector)
    {
      return false;
    }
  }

  private class NoBitmapDimensionPredicateFilter extends DimensionPredicateFilter
  {
    public NoBitmapDimensionPredicateFilter(
        final String dimension,
        final DruidPredicateFactory predicateFactory,
        final ExtractionFn extractionFn
    )
    {
      super(dimension, predicateFactory, extractionFn);
    }

    @Override
    public boolean supportsBitmapIndex(BitmapIndexSelector selector)
    {
      return false;
    }
  }

  private class NoBitmapSelectorDimFilter extends SelectorDimFilter
  {
    public NoBitmapSelectorDimFilter(
        String dimension,
        String value,
        ExtractionFn extractionFn
    )
    {
      super(dimension, value, extractionFn);
    }
    @Override
    public Filter toFilter()
    {
      ExtractionFn extractionFn = getExtractionFn();
      String dimension = getDimension();
      String value = getValue();
      if (extractionFn == null) {
        return new NoBitmapSelectorFilter(dimension, value);
      } else {
        final String valueOrNull = Strings.emptyToNull(value);
        final DruidPredicateFactory predicateFactory = new DruidPredicateFactory()
        {
          @Override
          public Predicate<String> makeStringPredicate()
          {
            return new Predicate<String>()
            {
              @Override
              public boolean apply(String input)
              {
                return Objects.equals(valueOrNull, input);
              }
            };
          }

          @Override
          public DruidLongPredicate makeLongPredicate()
          {
            return new DruidLongPredicate()
            {
              @Override
              public boolean applyLong(long input)
              {
                return Objects.equals(valueOrNull, String.valueOf(input));
              }
            };
          }
        };

        return new NoBitmapDimensionPredicateFilter(dimension, predicateFactory, extractionFn);
      }
    }
  }

  private static String JS_FN = "function(str) { return 'super-' + str; }";
  private static ExtractionFn JS_EXTRACTION_FN = new JavaScriptExtractionFn(JS_FN, false, JavaScriptConfig.getDefault());

  private static final String TIMESTAMP_COLUMN = "timestamp";

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", new DateTime("2000")),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim0", "dim1", "dim2", "dim3")),
              null,
              null
          )
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.of(
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "0", "dim1", "", "dim2", ImmutableList.of("a", "b"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "1", "dim1", "10", "dim2", ImmutableList.of())),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "2", "dim1", "2", "dim2", ImmutableList.of(""))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "3", "dim1", "1", "dim2", ImmutableList.of("a"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "4", "dim1", "def", "dim2", ImmutableList.of("c"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "5", "dim1", "abc")),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "6", "dim1", "B453B411", "dim2", ImmutableList.of("c", "d", "e"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "7", "dim1", "HELLO", "dim2", ImmutableList.of("foo"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "8", "dim1", "abc", "dim2", ImmutableList.of("bar"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "9", "dim1", "1", "dim2", ImmutableList.of("foo", "bar")))
  );

  public FilterPartitionTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean optimize
  )
  {
    super(testName, ROWS, indexBuilder, finisher, optimize);
  }

  @AfterClass
  public static void tearDown() throws Exception
  {
    BaseFilterTest.tearDown(FilterPartitionTest.class.getName());
  }

  @Test
  public void testSinglePreFilterWithNulls()
  {
    assertFilterMatches(new SelectorDimFilter("dim1", null, null), ImmutableList.of("0"));
    assertFilterMatches(new SelectorDimFilter("dim1", "", null), ImmutableList.of("0"));
    assertFilterMatches(new SelectorDimFilter("dim1", "10", null), ImmutableList.of("1"));
    assertFilterMatches(new SelectorDimFilter("dim1", "2", null), ImmutableList.of("2"));
    assertFilterMatches(new SelectorDimFilter("dim1", "1", null), ImmutableList.of("3", "9"));
    assertFilterMatches(new SelectorDimFilter("dim1", "def", null), ImmutableList.of("4"));
    assertFilterMatches(new SelectorDimFilter("dim1", "abc", null), ImmutableList.of("5", "8"));
    assertFilterMatches(new SelectorDimFilter("dim1", "ab", null), ImmutableList.<String>of());
  }

  @Test
  public void testSinglePostFilterWithNulls()
  {
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", null, null), ImmutableList.of("0"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "", null), ImmutableList.of("0"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "10", null), ImmutableList.of("1"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "2", null), ImmutableList.of("2"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "1", null), ImmutableList.of("3", "9"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "def", null), ImmutableList.of("4"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "abc", null), ImmutableList.of("5", "8"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "ab", null), ImmutableList.<String>of());

    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-null", JS_EXTRACTION_FN), ImmutableList.of("0"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-null", JS_EXTRACTION_FN), ImmutableList.of("0"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-10", JS_EXTRACTION_FN), ImmutableList.of("1"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-2", JS_EXTRACTION_FN), ImmutableList.of("2"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-1", JS_EXTRACTION_FN), ImmutableList.of("3", "9"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-def", JS_EXTRACTION_FN), ImmutableList.of("4"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-abc", JS_EXTRACTION_FN), ImmutableList.of("5", "8"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-ab", JS_EXTRACTION_FN), ImmutableList.<String>of());
  }

  @Test
  public void testBasicPreAndPostFilterWithNulls()
  {
    assertFilterMatches(
        new AndDimFilter(Arrays.<DimFilter>asList(
            new SelectorDimFilter("dim2", "a", null),
            new NoBitmapSelectorDimFilter("dim1", null, null)
        )),
        ImmutableList.of("0")
    );

    assertFilterMatches(
        new AndDimFilter(Arrays.<DimFilter>asList(
            new SelectorDimFilter("dim1", "10", null),
            new NoBitmapSelectorDimFilter("dim2", null, null)
        )),
        ImmutableList.of("1")
    );

    assertFilterMatches(
        new AndDimFilter(Arrays.<DimFilter>asList(
            new SelectorDimFilter("dim1", "1", null),
            new NoBitmapSelectorDimFilter("dim2", "foo", null)
        )),
        ImmutableList.of("9")
    );

    assertFilterMatches(
        new AndDimFilter(Arrays.<DimFilter>asList(
            new SelectorDimFilter("dim1", "HELLO", null),
            new NoBitmapSelectorDimFilter("dim2", "bar", null)
        )),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        new AndDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dim2", "bar", null),
            new SelectorDimFilter("dim1", "NOT_A_VALUE", null)
        )),
        ImmutableList.<String>of()
    );


    assertFilterMatches(
        new AndDimFilter(Arrays.<DimFilter>asList(
            new SelectorDimFilter("dim2", "super-a", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim1", "super-null", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("0")
    );

    assertFilterMatches(
        new AndDimFilter(Arrays.<DimFilter>asList(
            new SelectorDimFilter("dim1", "super-10", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim2", "super-null", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("1")
    );

    assertFilterMatches(
        new AndDimFilter(Arrays.<DimFilter>asList(
            new SelectorDimFilter("dim1", "super-2", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim2", "super-null", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("2")
    );

    assertFilterMatches(
        new AndDimFilter(Arrays.<DimFilter>asList(
            new SelectorDimFilter("dim1", "super-1", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim2", "super-foo", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("9")
    );

    assertFilterMatches(
        new AndDimFilter(Arrays.<DimFilter>asList(
            new SelectorDimFilter("dim1", "super-HELLO", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim2", "super-bar", JS_EXTRACTION_FN)
        )),
        ImmutableList.<String>of()
    );
  }

  @Test
  public void testOrPostFilterWithNulls()
  {
    assertFilterMatches(
        new OrDimFilter(Arrays.<DimFilter>asList(
            new SelectorDimFilter("dim2", "a", null),
            new NoBitmapSelectorDimFilter("dim1", null, null)
        )),
        ImmutableList.of("0", "3")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.<DimFilter>asList(
            new SelectorDimFilter("dim1", "abc", null),
            new NoBitmapSelectorDimFilter("dim2", null, null)
        )),
        ImmutableList.of("1", "2", "5", "8")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.<DimFilter>asList(
            new SelectorDimFilter("dim1", "2", null),
            new NoBitmapSelectorDimFilter("dim2", null, null)
        )),
        ImmutableList.of("1", "2", "5")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.<DimFilter>asList(
            new SelectorDimFilter("dim1", "INVALID_VALUE", null),
            new NoBitmapSelectorDimFilter("dim2", "foo", null)
        )),
        ImmutableList.of("7", "9")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.<DimFilter>asList(
            new SelectorDimFilter("dim1", "HELLO", null),
            new NoBitmapSelectorDimFilter("dim2", "bar", null)
        )),
        ImmutableList.<String>of("7", "8", "9")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dim1", "HELLO", null),
            new SelectorDimFilter("dim2", "NOT_A_VALUE", null)
        )),
        ImmutableList.<String>of("7")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dim1", "INVALID", null),
            new SelectorDimFilter("dim2", "NOT_A_VALUE", null)
        )),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.<DimFilter>asList(
            new SelectorDimFilter("dim2", "super-a", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim1", "super-null", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("0", "3")
    );
    assertFilterMatches(
        new OrDimFilter(Arrays.<DimFilter>asList(
            new SelectorDimFilter("dim1", "super-abc", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim2", "super-null", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("1", "2", "5", "8")
    );
    assertFilterMatches(
        new OrDimFilter(Arrays.<DimFilter>asList(
            new SelectorDimFilter("dim1", "super-2", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim2", "super-null", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("1", "2", "5")
    );
    assertFilterMatches(
        new OrDimFilter(Arrays.<DimFilter>asList(
            new SelectorDimFilter("dim1", "INVALID_VALUE", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim2", "super-foo", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("7", "9")
    );
    assertFilterMatches(
        new OrDimFilter(Arrays.<DimFilter>asList(
            new SelectorDimFilter("dim1", "super-HELLO", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim2", "super-bar", JS_EXTRACTION_FN)
        )),
        ImmutableList.<String>of("7", "8", "9")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dim1", "super-HELLO", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim2", "NOT_A_VALUE", null)
        )),
        ImmutableList.<String>of("7")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dim1", "INVALID", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim2", "NOT_A_VALUE", JS_EXTRACTION_FN)
        )),
        ImmutableList.<String>of()
    );
  }

  @Test
  public void testMissingColumnSpecifiedInDimensionList()
  {
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", null, null), ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "", null), ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "a", null), ImmutableList.<String>of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "b", null), ImmutableList.<String>of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "c", null), ImmutableList.<String>of());

    assertFilterMatches(
        new OrDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dim1", "abc", null),
            new SelectorDimFilter("dim3", "NOTHERE", null)
        )),
        ImmutableList.<String>of("5", "8")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dim1", "abc", null),
            new SelectorDimFilter("dim3", null, null)
        )),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
    );

    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "super-null", JS_EXTRACTION_FN),
                        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "super-null", JS_EXTRACTION_FN),
                        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "a", JS_EXTRACTION_FN), ImmutableList.<String>of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "b", JS_EXTRACTION_FN), ImmutableList.<String>of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "c", JS_EXTRACTION_FN), ImmutableList.<String>of());

    assertFilterMatches(
        new OrDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dim1", "super-abc", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim3", "NOTHERE", JS_EXTRACTION_FN)
        )),
        ImmutableList.<String>of("5", "8")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dim1", "abc", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim3", "super-null", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
    );
  }

  @Test
  public void testMissingColumnNotSpecifiedInDimensionList()
  {
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", null, null), ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "", null), ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "a", null), ImmutableList.<String>of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "b", null), ImmutableList.<String>of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "c", null), ImmutableList.<String>of());

    assertFilterMatches(
        new OrDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dim1", "abc", null),
            new SelectorDimFilter("dim4", null, null)
        )),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dim4", null, null),
            new SelectorDimFilter("dim1", "abc", null)
        )),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
    );

    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "super-null", JS_EXTRACTION_FN),
                        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "super-null", JS_EXTRACTION_FN),
                        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "a", JS_EXTRACTION_FN), ImmutableList.<String>of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "b", JS_EXTRACTION_FN), ImmutableList.<String>of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "c", JS_EXTRACTION_FN), ImmutableList.<String>of());

    assertFilterMatches(
        new OrDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dim1", "super-abc", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim4", "super-null", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dim4", "super-null", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim1", "super-abc", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
    );
  }

  @Test
  public void testDistributeOrCNF()
  {
    DimFilter dimFilter1 = new OrDimFilter(Arrays.<DimFilter>asList(
        new SelectorDimFilter("dim0", "6", null),
        new AndDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dim1", "def", null),
            new SelectorDimFilter("dim2", "c", null)
        )
        ))
    );

    Filter filter1 = dimFilter1.toFilter();
    Filter filter1CNF = Filters.convertToCNF(filter1);

    Assert.assertEquals(AndFilter.class, filter1CNF.getClass());
    Assert.assertEquals(2, ((AndFilter) filter1CNF).getFilters().size());

    assertFilterMatches(
        dimFilter1,
        ImmutableList.of("4", "6")
    );

    assertFilterMatchesCNF(
        dimFilter1,
        ImmutableList.of("4", "6")
    );


    DimFilter dimFilter2 = new OrDimFilter(Arrays.<DimFilter>asList(
        new SelectorDimFilter("dim0", "2", null),
        new SelectorDimFilter("dim0", "3", null),
        new AndDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dim1", "HELLO", null),
            new SelectorDimFilter("dim2", "foo", null)
        )
        ))
    );

    assertFilterMatches(
        dimFilter2,
        ImmutableList.of("2", "3", "7")
    );

    assertFilterMatchesCNF(
        dimFilter2,
        ImmutableList.of("2", "3", "7")
    );


    DimFilter dimFilter3 = new OrDimFilter(Arrays.<DimFilter>asList(
        dimFilter1,
        dimFilter2,
        new AndDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dim1", "1", null),
            new SelectorDimFilter("dim2", "foo", null)
        )
        ))
    );

    Filter filter3 = dimFilter3.toFilter();
    Filter filter3CNF = Filters.convertToCNF(dimFilter3.toFilter());

    assertFilterMatches(
        dimFilter3,
        ImmutableList.of("2", "3", "4", "6", "7", "9")
    );

    assertFilterMatchesCNF(
        dimFilter3,
        ImmutableList.of("2", "3", "4", "6", "7", "9")
    );
  }


  @Test
  public void testDistributeOrCNFExtractionFn()
  {
    DimFilter dimFilter1 = new OrDimFilter(Arrays.<DimFilter>asList(
        new SelectorDimFilter("dim0", "super-6", JS_EXTRACTION_FN),
        new AndDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dim1", "super-def", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim2", "super-c", JS_EXTRACTION_FN)
        )
        ))
    );

    Filter filter1 = dimFilter1.toFilter();
    Filter filter1CNF = Filters.convertToCNF(filter1);

    Assert.assertEquals(AndFilter.class, filter1CNF.getClass());
    Assert.assertEquals(2, ((AndFilter) filter1CNF).getFilters().size());

    assertFilterMatches(
        dimFilter1,
        ImmutableList.of("4", "6")
    );

    assertFilterMatchesCNF(
        dimFilter1,
        ImmutableList.of("4", "6")
    );


    DimFilter dimFilter2 = new OrDimFilter(Arrays.<DimFilter>asList(
        new SelectorDimFilter("dim0", "super-2", JS_EXTRACTION_FN),
        new SelectorDimFilter("dim0", "super-3", JS_EXTRACTION_FN),
        new AndDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dim1", "super-HELLO", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim2", "super-foo", JS_EXTRACTION_FN)
        )
        ))
    );

    assertFilterMatches(
        dimFilter2,
        ImmutableList.of("2", "3", "7")
    );

    assertFilterMatchesCNF(
        dimFilter2,
        ImmutableList.of("2", "3", "7")
    );


    DimFilter dimFilter3 = new OrDimFilter(Arrays.<DimFilter>asList(
        dimFilter1,
        dimFilter2,
        new AndDimFilter(Arrays.<DimFilter>asList(
            new NoBitmapSelectorDimFilter("dim1", "super-1", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim2", "super-foo", JS_EXTRACTION_FN)
        )
        ))
    );

    Filter filter3 = dimFilter3.toFilter();
    Filter filter3CNF = Filters.convertToCNF(dimFilter3.toFilter());

    assertFilterMatches(
        dimFilter3,
        ImmutableList.of("2", "3", "4", "6", "7", "9")
    );

    assertFilterMatchesCNF(
        dimFilter3,
        ImmutableList.of("2", "3", "4", "6", "7", "9")
    );
  }


  private void assertFilterMatches(
      final DimFilter filter,
      final List<String> expectedRows
  )
  {
    Assert.assertEquals(filter.toString(), expectedRows, selectColumnValuesMatchingFilter(filter, "dim0"));
    Assert.assertEquals(filter.toString(), expectedRows.size(), selectCountUsingFilteredAggregator(filter));
  }

  private void assertFilterMatchesCNF(
      final DimFilter filter,
      final List<String> expectedRows
  )
  {
    Assert.assertEquals(filter.toString(), expectedRows, selectColumnValuesMatchingFilterCNF(filter, "dim0"));
    Assert.assertEquals(filter.toString(), expectedRows.size(), selectCountUsingFilteredAggregator(filter));
  }

  protected List<String> selectColumnValuesMatchingFilterCNF(final DimFilter dimFilter, final String selectColumn)
  {
    final Filter filter = Filters.convertToCNF(maybeOptimize(dimFilter).toFilter());

    final Sequence<Cursor> cursors = makeCursorSequence(filter);
    Sequence<List<String>> seq = Sequences.map(
        cursors,
        new Function<Cursor, List<String>>()
        {
          @Override
          public List<String> apply(Cursor input)
          {
            final DimensionSelector selector = input.makeDimensionSelector(
                new DefaultDimensionSpec(selectColumn, selectColumn)
            );

            final List<String> values = Lists.newArrayList();

            while (!input.isDone()) {
              IndexedInts row = selector.getRow();
              Preconditions.checkState(row.size() == 1);
              values.add(selector.lookupName(row.get(0)));
              input.advance();
            }

            return values;
          }
        }
    );
    return Sequences.toList(seq, new ArrayList<List<String>>()).get(0);
  }

}
