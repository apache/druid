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

package org.apache.druid.segment.filter;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.JavaScriptExtractionFn;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.DruidPredicateMatch;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterTuning;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.ColumnSelectorColumnIndexSelector;
import org.apache.druid.segment.FilterAnalysis;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.filter.cnf.CNFFilterExplosionException;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@RunWith(Parameterized.class)
public class FilterPartitionTest extends BaseFilterTest
{
  private static class NoBitmapSelectorFilter extends SelectorFilter
  {
    public NoBitmapSelectorFilter(
        String dimension,
        String value
    )
    {
      super(dimension, value);
    }

    public NoBitmapSelectorFilter(
        String dimension,
        String value,
        FilterTuning filterTuning
    )
    {
      super(dimension, value, filterTuning);
    }

    @Nullable
    @Override
    public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
    {
      return null;
    }
  }

  private static class NoBitmapDimensionPredicateFilter extends DimensionPredicateFilter
  {
    public NoBitmapDimensionPredicateFilter(
        final String dimension,
        final DruidPredicateFactory predicateFactory,
        final ExtractionFn extractionFn
    )
    {
      super(dimension, predicateFactory, extractionFn);
    }

    @Nullable
    @Override
    public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
    {
      return null;
    }
  }

  private static class NoBitmapSelectorDimFilter extends SelectorDimFilter
  {
    NoBitmapSelectorDimFilter(String dimension, String value, ExtractionFn extractionFn)
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
        final String valueOrNull = NullHandling.emptyToNullIfNeeded(value);
        final DruidPredicateFactory predicateFactory = new DruidPredicateFactory()
        {
          @Override
          public DruidObjectPredicate<String> makeStringPredicate()
          {
            return valueOrNull == null ? DruidObjectPredicate.isNull() : DruidObjectPredicate.equalTo(valueOrNull);
          }

          @Override
          public DruidLongPredicate makeLongPredicate()
          {
            return input -> DruidPredicateMatch.of(Objects.equals(valueOrNull, String.valueOf(input)));
          }

          @Override
          public DruidFloatPredicate makeFloatPredicate()
          {
            return input -> DruidPredicateMatch.of(Objects.equals(valueOrNull, String.valueOf(input)));
          }

          @Override
          public DruidDoublePredicate makeDoublePredicate()
          {
            return input -> DruidPredicateMatch.of(Objects.equals(valueOrNull, String.valueOf(input)));
          }

        };

        return new NoBitmapDimensionPredicateFilter(dimension, predicateFactory, extractionFn);
      }
    }
  }

  private static final String JS_FN = "function(str) { return 'super-' + str; }";
  private static final ExtractionFn JS_EXTRACTION_FN =
      new JavaScriptExtractionFn(JS_FN, false, JavaScriptConfig.getEnabledInstance());

  private static final List<InputRow> ROWS = ImmutableList.<InputRow>builder()
      .addAll(DEFAULT_ROWS)
      .add(makeDefaultSchemaRow("6", "B453B411", ImmutableList.of("c", "d", "e"), null, null, null, null, null))
      .add(makeDefaultSchemaRow("7", "HELLO", ImmutableList.of("foo"), null, null, null, null, null))
      .add(makeDefaultSchemaRow("8", "abc", ImmutableList.of("bar"), null, null, null, null, null))
      .add(makeDefaultSchemaRow("9", "1", ImmutableList.of("foo", "bar"), null, null, null, null, null))
      .build();

  public FilterPartitionTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean cnf,
      boolean optimize
  )
  {
    super(testName, ROWS, indexBuilder, finisher, cnf, optimize);
  }

  @AfterClass
  public static void tearDown() throws Exception
  {
    BaseFilterTest.tearDown(FilterPartitionTest.class.getName());
  }

  @Test
  public void testSinglePreFilterWithNulls()
  {
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(new SelectorDimFilter("dim1", null, null), ImmutableList.of("0"));
    } else {
      assertFilterMatches(new SelectorDimFilter("dim1", null, null), ImmutableList.of());
    }
    assertFilterMatches(new SelectorDimFilter("dim1", "", null), ImmutableList.of("0"));
    assertFilterMatches(new SelectorDimFilter("dim1", "10", null), ImmutableList.of("1"));
    assertFilterMatches(new SelectorDimFilter("dim1", "2", null), ImmutableList.of("2"));
    assertFilterMatches(new SelectorDimFilter("dim1", "1", null), ImmutableList.of("3", "9"));
    assertFilterMatches(new SelectorDimFilter("dim1", "abdef", null), ImmutableList.of("4"));
    assertFilterMatches(new SelectorDimFilter("dim1", "abc", null), ImmutableList.of("5", "8"));
    assertFilterMatches(new SelectorDimFilter("dim1", "ab", null), ImmutableList.of());
  }

  @Test
  public void testSinglePostFilterWithNulls()
  {
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", null, null), ImmutableList.of("0"));
    } else {
      assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", null, null), ImmutableList.of());
    }
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "", null), ImmutableList.of("0"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "10", null), ImmutableList.of("1"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "2", null), ImmutableList.of("2"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "1", null), ImmutableList.of("3", "9"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "abdef", null), ImmutableList.of("4"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "abc", null), ImmutableList.of("5", "8"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "ab", null), ImmutableList.of());

    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-null", JS_EXTRACTION_FN), ImmutableList.of("0"));
    } else {
      assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-", JS_EXTRACTION_FN), ImmutableList.of("0"));
    }
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-10", JS_EXTRACTION_FN), ImmutableList.of("1"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-2", JS_EXTRACTION_FN), ImmutableList.of("2"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-1", JS_EXTRACTION_FN), ImmutableList.of("3", "9"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-abdef", JS_EXTRACTION_FN), ImmutableList.of("4"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-abc", JS_EXTRACTION_FN), ImmutableList.of("5", "8"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-ab", JS_EXTRACTION_FN), ImmutableList.of());
  }

  @Test
  public void testBasicPreAndPostFilterWithNulls()
  {
    if (isAutoSchema()) {
      return;
    }
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          new AndDimFilter(Arrays.asList(
              new SelectorDimFilter("dim2", "a", null),
              new NoBitmapSelectorDimFilter("dim1", null, null)
          )),
          ImmutableList.of("0")
      );
    } else {
      assertFilterMatches(
          new AndDimFilter(Arrays.asList(
              new SelectorDimFilter("dim2", "a", null),
              new NoBitmapSelectorDimFilter("dim1", null, null)
          )),
          ImmutableList.of()
      );
    }

    assertFilterMatches(
        new AndDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "10", null),
            new NoBitmapSelectorDimFilter("dim2", null, null)
        )),
        ImmutableList.of("1")
    );

    assertFilterMatches(
        new AndDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "1", null),
            new NoBitmapSelectorDimFilter("dim2", "foo", null)
        )),
        ImmutableList.of("9")
    );

    assertFilterMatches(
        new AndDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "HELLO", null),
            new NoBitmapSelectorDimFilter("dim2", "bar", null)
        )),
        ImmutableList.of()
    );

    assertFilterMatches(
        new AndDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim2", "bar", null),
            new SelectorDimFilter("dim1", "NOT_A_VALUE", null)
        )),
        ImmutableList.of()
    );

    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          new AndDimFilter(Arrays.asList(
              new SelectorDimFilter("dim2", "super-a", JS_EXTRACTION_FN),
              new NoBitmapSelectorDimFilter("dim1", "super-null", JS_EXTRACTION_FN)
          )),
          ImmutableList.of("0")
      );
      assertFilterMatches(
          new AndDimFilter(Arrays.asList(
              new SelectorDimFilter("dim1", "super-2", JS_EXTRACTION_FN),
              new NoBitmapSelectorDimFilter("dim2", "super-null", JS_EXTRACTION_FN)
          )),
          ImmutableList.of("2")
      );
    } else {
      assertFilterMatches(
          new AndDimFilter(Arrays.asList(
              new SelectorDimFilter("dim2", "super-a", JS_EXTRACTION_FN),
              new NoBitmapSelectorDimFilter("dim1", "super-", JS_EXTRACTION_FN)
          )),
          ImmutableList.of("0")
      );
      assertFilterMatches(
          new AndDimFilter(Arrays.asList(
              new SelectorDimFilter("dim2", "super-a", JS_EXTRACTION_FN),
              new NoBitmapSelectorDimFilter("dim1", "super-null", JS_EXTRACTION_FN)
          )),
          ImmutableList.of()
      );
      assertFilterMatches(
          new AndDimFilter(Arrays.asList(
              new SelectorDimFilter("dim1", "super-2", JS_EXTRACTION_FN),
              new NoBitmapSelectorDimFilter("dim2", "super-", JS_EXTRACTION_FN)
          )),
          ImmutableList.of("2")
      );
      assertFilterMatches(
          new AndDimFilter(Arrays.asList(
              new SelectorDimFilter("dim1", "super-2", JS_EXTRACTION_FN),
              new NoBitmapSelectorDimFilter("dim2", "super-null", JS_EXTRACTION_FN)
          )),
          ImmutableList.of()
      );
    }

    assertFilterMatches(
        new AndDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "super-10", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim2", "super-null", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("1")
    );

    assertFilterMatches(
        new AndDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "super-1", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim2", "super-foo", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("9")
    );

    assertFilterMatches(
        new AndDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "super-HELLO", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim2", "super-bar", JS_EXTRACTION_FN)
        )),
        ImmutableList.of()
    );
  }

  @Test
  public void testOrPostFilterWithNulls()
  {
    if (isAutoSchema()) {
      return;
    }
    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new SelectorDimFilter("dim2", "a", null),
            new NoBitmapSelectorDimFilter("dim1", null, null)
        )),
        ImmutableList.of("0", "3")
    );

    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          new OrDimFilter(Arrays.asList(
              new SelectorDimFilter("dim1", "abc", null),
              new NoBitmapSelectorDimFilter("dim2", null, null)
          )),
          ImmutableList.of("1", "2", "5", "8")
      );
    } else {
      assertFilterMatches(
          new OrDimFilter(Arrays.asList(
              new SelectorDimFilter("dim1", "abc", null),
              new NoBitmapSelectorDimFilter("dim2", null, null)
          )),
          ImmutableList.of("1", "5", "8")
      );
    }

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "2", null),
            new NoBitmapSelectorDimFilter("dim2", null, null)
        )),
        ImmutableList.of("1", "2", "5")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "INVALID_VALUE", null),
            new NoBitmapSelectorDimFilter("dim2", "foo", null)
        )),
        ImmutableList.of("7", "9")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "HELLO", null),
            new NoBitmapSelectorDimFilter("dim2", "bar", null)
        )),
        ImmutableList.of("7", "8", "9")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "HELLO", null),
            new SelectorDimFilter("dim2", "NOT_A_VALUE", null)
        )),
        ImmutableList.of("7")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "INVALID", null),
            new SelectorDimFilter("dim2", "NOT_A_VALUE", null)
        )),
        ImmutableList.of()
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new SelectorDimFilter("dim2", "super-a", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim1", "super-null", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("0", "3")
    );

    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          new OrDimFilter(Arrays.asList(
              new SelectorDimFilter("dim1", "super-abc", JS_EXTRACTION_FN),
              new NoBitmapSelectorDimFilter("dim2", "super-null", JS_EXTRACTION_FN)
          )),
          ImmutableList.of("1", "2", "5", "8")
      );
    } else {
      assertFilterMatches(
          new OrDimFilter(Arrays.asList(
              new SelectorDimFilter("dim1", "super-abc", JS_EXTRACTION_FN),
              new NoBitmapSelectorDimFilter("dim2", "super-null", JS_EXTRACTION_FN)
          )),
          ImmutableList.of("1", "5", "8")
      );
      assertFilterMatches(
          new OrDimFilter(Arrays.asList(
              new SelectorDimFilter("dim1", "super-abc", JS_EXTRACTION_FN),
              new NoBitmapSelectorDimFilter("dim2", "super-", JS_EXTRACTION_FN)
          )),
          ImmutableList.of("2", "5", "8")
      );
    }

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "super-2", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim2", "super-null", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("1", "2", "5")
    );
    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "INVALID_VALUE", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim2", "super-foo", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("7", "9")
    );
    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "super-HELLO", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim2", "super-bar", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("7", "8", "9")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "super-HELLO", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim2", "NOT_A_VALUE", null)
        )),
        ImmutableList.of("7")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "INVALID", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim2", "NOT_A_VALUE", JS_EXTRACTION_FN)
        )),
        ImmutableList.of()
    );
  }

  @Test
  public void testMissingColumnSpecifiedInDimensionList()
  {
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", null, null), ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          new NoBitmapSelectorDimFilter("dim3", "", null),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
      );
    } else {
      assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "", null), ImmutableList.of());
    }
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "a", null), ImmutableList.of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "b", null), ImmutableList.of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "c", null), ImmutableList.of());

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "abc", null),
            new SelectorDimFilter("dim3", "NOTHERE", null)
        )),
        ImmutableList.of("5", "8")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "abc", null),
            new SelectorDimFilter("dim3", null, null)
        )),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
    );

    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "super-null", JS_EXTRACTION_FN),
                        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "super-null", JS_EXTRACTION_FN),
                        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "a", JS_EXTRACTION_FN), ImmutableList.of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "b", JS_EXTRACTION_FN), ImmutableList.of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "c", JS_EXTRACTION_FN), ImmutableList.of());

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "super-abc", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim3", "NOTHERE", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("5", "8")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
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
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          new NoBitmapSelectorDimFilter("dim4", "", null),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
      );
    } else {
      assertFilterMatches(
          new NoBitmapSelectorDimFilter("dim4", "", null),
          ImmutableList.of()
      );
    }
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "a", null), ImmutableList.of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "b", null), ImmutableList.of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "c", null), ImmutableList.of());

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "abc", null),
            new SelectorDimFilter("dim4", null, null)
        )),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim4", null, null),
            new SelectorDimFilter("dim1", "abc", null)
        )),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
    );

    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "super-null", JS_EXTRACTION_FN),
                        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "super-null", JS_EXTRACTION_FN),
                        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "a", JS_EXTRACTION_FN), ImmutableList.of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "b", JS_EXTRACTION_FN), ImmutableList.of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "c", JS_EXTRACTION_FN), ImmutableList.of());

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "super-abc", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim4", "super-null", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim4", "super-null", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim1", "super-abc", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
    );
  }

  @Test
  public void testDistributeOrCNF() throws CNFFilterExplosionException
  {
    if (isAutoSchema()) {
      return;
    }
    DimFilter dimFilter1 = new OrDimFilter(Arrays.asList(
        new SelectorDimFilter("dim0", "6", null),
        new AndDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "abdef", null),
            new SelectorDimFilter("dim2", "c", null)
        )
        )
    )
    );

    Filter filter1 = dimFilter1.toFilter();
    Filter filter1CNF = Filters.toCnf(filter1);

    Assert.assertEquals(AndFilter.class, filter1CNF.getClass());
    Assert.assertEquals(2, ((AndFilter) filter1CNF).getFilters().size());

    assertFilterMatches(
        dimFilter1,
        ImmutableList.of("4", "6")
    );

    DimFilter dimFilter2 = new OrDimFilter(Arrays.asList(
        new SelectorDimFilter("dim0", "2", null),
        new SelectorDimFilter("dim0", "3", null),
        new AndDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "HELLO", null),
            new SelectorDimFilter("dim2", "foo", null)
        )
        ))
    );

    assertFilterMatches(
        dimFilter2,
        ImmutableList.of("2", "3", "7")
    );

    DimFilter dimFilter3 = new OrDimFilter(Arrays.asList(
        dimFilter1,
        dimFilter2,
        new AndDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "1", null),
            new SelectorDimFilter("dim2", "foo", null)
        )
        ))
    );

    assertFilterMatches(
        dimFilter3,
        ImmutableList.of("2", "3", "4", "6", "7", "9")
    );
  }

  @Test
  public void testDistributeOrCNFExtractionFn() throws CNFFilterExplosionException
  {
    if (isAutoSchema()) {
      return;
    }
    DimFilter dimFilter1 = new OrDimFilter(Arrays.asList(
        new SelectorDimFilter("dim0", "super-6", JS_EXTRACTION_FN),
        new AndDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "super-abdef", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim2", "super-c", JS_EXTRACTION_FN)
        )
        )
    )
    );

    Filter filter1 = dimFilter1.toFilter();
    Filter filter1CNF = Filters.toCnf(filter1);

    Assert.assertEquals(AndFilter.class, filter1CNF.getClass());
    Assert.assertEquals(2, ((AndFilter) filter1CNF).getFilters().size());

    assertFilterMatches(
        dimFilter1,
        ImmutableList.of("4", "6")
    );

    DimFilter dimFilter2 = new OrDimFilter(Arrays.asList(
        new SelectorDimFilter("dim0", "super-2", JS_EXTRACTION_FN),
        new SelectorDimFilter("dim0", "super-3", JS_EXTRACTION_FN),
        new AndDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "super-HELLO", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim2", "super-foo", JS_EXTRACTION_FN)
        )
        ))
    );

    assertFilterMatches(
        dimFilter2,
        ImmutableList.of("2", "3", "7")
    );

    DimFilter dimFilter3 = new OrDimFilter(
        dimFilter1,
        dimFilter2,
        new AndDimFilter(
            new NoBitmapSelectorDimFilter("dim1", "super-1", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim2", "super-foo", JS_EXTRACTION_FN)
        )
    );

    assertFilterMatches(
        dimFilter3,
        ImmutableList.of("2", "3", "4", "6", "7", "9")
    );
  }

  @Test
  public void testAnalyze()
  {
    if (!(adapter instanceof QueryableIndexStorageAdapter)) {
      return;
    }
    QueryableIndexStorageAdapter storageAdapter = (QueryableIndexStorageAdapter) adapter;
    final int numRows = adapter.getNumRows();

    final ColumnSelectorColumnIndexSelector bitmapIndexSelector = storageAdapter.makeBitmapIndexSelector(BaseFilterTest.VIRTUAL_COLUMNS);

    // has bitmap index, will use it by default
    Filter normalFilter = new SelectorFilter("dim1", "HELLO");
    FilterAnalysis filterAnalysisNormal =
        FilterAnalysis.analyzeFilter(normalFilter, bitmapIndexSelector, null, numRows);
    Assert.assertTrue(filterAnalysisNormal.getPreFilterBitmap() != null);
    Assert.assertTrue(filterAnalysisNormal.getPostFilter() == null);


    // no bitmap index, should be a post filter
    Filter noBitmapFilter = new NoBitmapSelectorFilter("dim1", "HELLO");
    FilterAnalysis noBitmapFilterAnalysis =
        FilterAnalysis.analyzeFilter(noBitmapFilter, bitmapIndexSelector, null, numRows);
    Assert.assertTrue(noBitmapFilterAnalysis.getPreFilterBitmap() == null);
    Assert.assertTrue(noBitmapFilterAnalysis.getPostFilter() != null);

    // this column has a bitmap index, but is forced to not use it
    Filter bitmapFilterWithForceNoIndexTuning = new SelectorFilter(
        "dim1",
        "HELLO",
        new FilterTuning(false, null, null)
    );
    FilterAnalysis bitmapFilterWithForceNoIndexTuningAnalysis =
        FilterAnalysis.analyzeFilter(bitmapFilterWithForceNoIndexTuning, bitmapIndexSelector, null, numRows);
    Assert.assertTrue(bitmapFilterWithForceNoIndexTuningAnalysis.getPreFilterBitmap() == null);
    Assert.assertTrue(bitmapFilterWithForceNoIndexTuningAnalysis.getPostFilter() != null);

    // this max cardinality is too low to use bitmap index
    Filter bitmapFilterWithCardinalityMax = new SelectorFilter(
        "dim1",
        "HELLO",
        new FilterTuning(true, 0, 3)
    );
    FilterAnalysis bitmapFilterWithCardinalityMaxAnalysis =
        FilterAnalysis.analyzeFilter(bitmapFilterWithCardinalityMax, bitmapIndexSelector, null, numRows);
    Assert.assertTrue(bitmapFilterWithCardinalityMaxAnalysis.getPreFilterBitmap() == null);
    Assert.assertTrue(bitmapFilterWithCardinalityMaxAnalysis.getPostFilter() != null);

    // this max cardinality is high enough that we can still use bitmap index
    Filter bitmapFilterWithCardinalityMax2 = new SelectorFilter(
        "dim1",
        "HELLO",
        new FilterTuning(true, 0, 1000)
    );
    FilterAnalysis bitmapFilterWithCardinalityMax2Analysis =
        FilterAnalysis.analyzeFilter(bitmapFilterWithCardinalityMax2, bitmapIndexSelector, null, numRows);
    Assert.assertTrue(bitmapFilterWithCardinalityMax2Analysis.getPreFilterBitmap() != null);
    Assert.assertTrue(bitmapFilterWithCardinalityMax2Analysis.getPostFilter() == null);

    // this min cardinality is too high, will not use bitmap index
    Filter bitmapFilterWithCardinalityMin = new SelectorFilter(
        "dim1",
        "HELLO",
        new FilterTuning(true, 1000, null)
    );
    FilterAnalysis bitmapFilterWithCardinalityMinAnalysis =
        FilterAnalysis.analyzeFilter(bitmapFilterWithCardinalityMin, bitmapIndexSelector, null, numRows);
    Assert.assertTrue(bitmapFilterWithCardinalityMinAnalysis.getPreFilterBitmap() == null);
    Assert.assertTrue(bitmapFilterWithCardinalityMinAnalysis.getPostFilter() != null);

    // cannot force using bitmap if there are no bitmaps
    Filter noBitmapFilterWithForceUse = new NoBitmapSelectorFilter(
        "dim1",
        "HELLO",
        new FilterTuning(true, null, null)
    );
    FilterAnalysis noBitmapFilterWithForceUseAnalysis =
        FilterAnalysis.analyzeFilter(noBitmapFilterWithForceUse, bitmapIndexSelector, null, numRows);
    Assert.assertTrue(noBitmapFilterWithForceUseAnalysis.getPreFilterBitmap() == null);
    Assert.assertTrue(noBitmapFilterWithForceUseAnalysis.getPostFilter() != null);
  }
}
