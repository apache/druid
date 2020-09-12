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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.filter.cnf.CalciteCnfHelper;
import org.apache.druid.segment.filter.cnf.HiveCnfHelper;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class FilterCnfConversionTest
{
  @Test
  public void testPushDownNot()
  {
    final Filter filter = FilterTestUtils.not(
        FilterTestUtils.and(
            FilterTestUtils.selector("col1", "1"),
            FilterTestUtils.selector("col2", "2"),
            FilterTestUtils.not(FilterTestUtils.selector("col3", "3"))
        )
    );
    final Filter expected = FilterTestUtils.or(
        FilterTestUtils.not(FilterTestUtils.selector("col1", "1")),
        FilterTestUtils.not(FilterTestUtils.selector("col2", "2")),
        FilterTestUtils.selector("col3", "3")
    );
    final Filter pushedDown = HiveCnfHelper.pushDownNot(filter);
    assertFilter(filter, expected, pushedDown);
  }

  @Test
  public void testPushDownNotLeafNot()
  {
    final Filter filter = FilterTestUtils.and(
        FilterTestUtils.selector("col1", "1"),
        FilterTestUtils.selector("col2", "2"),
        FilterTestUtils.not(FilterTestUtils.selector("col3", "3"))
    );
    final Filter pushedDown = HiveCnfHelper.pushDownNot(filter);
    assertFilter(filter, filter, pushedDown);
  }

  @Test
  public void testFlatten()
  {
    final Filter filter = FilterTestUtils.and(
        FilterTestUtils.and(
            FilterTestUtils.and(
                FilterTestUtils.selector("col1", "1"),
                FilterTestUtils.selector("col2", "2")
            )
        ),
        FilterTestUtils.selector("col3", "3")
    );
    final Filter expected = FilterTestUtils.and(
        FilterTestUtils.selector("col1", "1"),
        FilterTestUtils.selector("col2", "2"),
        FilterTestUtils.selector("col3", "3")
    );
    final Filter flattened = HiveCnfHelper.flatten(filter);
    assertFilter(filter, expected, flattened);
  }

  @Test
  public void testFlattenUnflattenable()
  {
    final Filter filter = FilterTestUtils.and(
        FilterTestUtils.or(
            FilterTestUtils.selector("col1", "1"),
            FilterTestUtils.selector("col2", "2")
        ),
        FilterTestUtils.selector("col3", "3")
    );
    final Filter flattened = HiveCnfHelper.flatten(filter);
    assertFilter(filter, filter, flattened);
  }

  @Test
  public void testToCnfWithMuchReducibleFilter()
  {
    final Filter muchReducible = FilterTestUtils.and(
        // should be flattened
        FilterTestUtils.and(
            FilterTestUtils.and(
                FilterTestUtils.and(FilterTestUtils.selector("col1", "val1"))
            )
        ),
        // should be flattened
        FilterTestUtils.and(
            FilterTestUtils.or(
                FilterTestUtils.and(FilterTestUtils.selector("col1", "val1"))
            )
        ),
        // should be flattened
        FilterTestUtils.or(
            FilterTestUtils.and(
                FilterTestUtils.or(FilterTestUtils.selector("col1", "val1"))
            )
        ),
        // should eliminate duplicate filters
        FilterTestUtils.selector("col1", "val1"),
        FilterTestUtils.selector("col2", "val2"),
        FilterTestUtils.and(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.selector("col2", "val2")
        ),
        FilterTestUtils.and(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.and(
                FilterTestUtils.selector("col2", "val2"),
                FilterTestUtils.selector("col1", "val1")
            )
        )
    );
    final Filter expected = FilterTestUtils.and(
        FilterTestUtils.selector("col1", "val1"),
        FilterTestUtils.selector("col2", "val2")
    );
    final Filter cnf = Filters.toCnf(muchReducible);
    assertFilter(muchReducible, expected, cnf);
  }

  @Test
  public void testToNormalizedOrClausesWithMuchReducibleFilter()
  {
    final Filter muchReducible = FilterTestUtils.and(
        // should be flattened
        FilterTestUtils.and(
            FilterTestUtils.and(
                FilterTestUtils.and(FilterTestUtils.selector("col1", "val1"))
            )
        ),
        // should be flattened
        FilterTestUtils.and(
            FilterTestUtils.or(
                FilterTestUtils.and(FilterTestUtils.selector("col1", "val1"))
            )
        ),
        // should be flattened
        FilterTestUtils.or(
            FilterTestUtils.and(
                FilterTestUtils.or(FilterTestUtils.selector("col1", "val1"))
            )
        ),
        // should eliminate duplicate filters
        FilterTestUtils.selector("col1", "val1"),
        FilterTestUtils.selector("col2", "val2"),
        FilterTestUtils.and(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.selector("col2", "val2")
        ),
        FilterTestUtils.and(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.and(
                FilterTestUtils.selector("col2", "val2"),
                FilterTestUtils.selector("col1", "val1")
            )
        )
    );
    final Set<Filter> expected = ImmutableSet.of(
        FilterTestUtils.selector("col1", "val1"),
        FilterTestUtils.selector("col2", "val2")
    );
    final Set<Filter> normalizedOrClauses = Filters.toNormalizedOrClauses(muchReducible);
    Assert.assertEquals(expected, normalizedOrClauses);
  }

  @Test
  public void testToCnfWithComplexFilterIncludingNotAndOr()
  {
    final Filter filter = FilterTestUtils.and(
        FilterTestUtils.or(
            FilterTestUtils.and(
                FilterTestUtils.selector("col1", "val1"),
                FilterTestUtils.selector("col2", "val2")
            ),
            FilterTestUtils.not(
                FilterTestUtils.and(
                    FilterTestUtils.selector("col4", "val4"),
                    FilterTestUtils.selector("col5", "val5")
                )
            )
        ),
        FilterTestUtils.or(
            FilterTestUtils.not(
                FilterTestUtils.or(
                    FilterTestUtils.selector("col2", "val2"),
                    FilterTestUtils.selector("col4", "val4"),
                    FilterTestUtils.selector("col5", "val5")
                )
            ),
            FilterTestUtils.and(
                FilterTestUtils.selector("col1", "val1"),
                FilterTestUtils.selector("col3", "val3")
            )
        ),
        FilterTestUtils.and(
            FilterTestUtils.or(
                FilterTestUtils.selector("col1", "val1"),
                FilterTestUtils.selector("col2", "val22"), // selecting different value
                FilterTestUtils.selector("col3", "val3")
            ),
            FilterTestUtils.not(
                FilterTestUtils.selector("col1", "val11")
            )
        ),
        FilterTestUtils.and(
            FilterTestUtils.or(
                FilterTestUtils.selector("col1", "val1"),
                FilterTestUtils.selector("col2", "val22"),
                FilterTestUtils.selector("col3", "val3")
            ),
            FilterTestUtils.not(
                FilterTestUtils.selector("col1", "val11") // selecting different value
            )
        )
    );
    final Filter expected = FilterTestUtils.and(
        FilterTestUtils.or(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.selector("col2", "val22"),
            FilterTestUtils.selector("col3", "val3")
        ),
        FilterTestUtils.or(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.not(FilterTestUtils.selector("col2", "val2"))
        ),
        FilterTestUtils.or(
            FilterTestUtils.not(FilterTestUtils.selector("col2", "val2")),
            FilterTestUtils.selector("col3", "val3")
        ),
        FilterTestUtils.or(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.not(FilterTestUtils.selector("col4", "val4"))
        ),
        FilterTestUtils.or(
            FilterTestUtils.selector("col3", "val3"),
            FilterTestUtils.not(FilterTestUtils.selector("col4", "val4"))
        ),
        FilterTestUtils.or(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.not(FilterTestUtils.selector("col5", "val5"))
        ),
        FilterTestUtils.or(
            FilterTestUtils.selector("col3", "val3"),
            FilterTestUtils.not(FilterTestUtils.selector("col5", "val5"))
        ),
        FilterTestUtils.not(FilterTestUtils.selector("col1", "val11")),
        // The below OR filter could be eliminated because this filter also has
        // (col1 = val1 || ~(col4 = val4)) && (col1 = val1 || ~(col5 = val5)).
        // The reduction process would be
        // (col1 = val1 || ~(col4 = val4)) && (col1 = val1 || ~(col5 = val5)) && (col1 = val1 || ~(col4 = val4) || ~(col5 = val5))
        // => (col1 = val1 && ~(col4 = val4) || ~(col5 = val5)) && (col1 = val1 || ~(col4 = val4) || ~(col5 = val5))
        // => (col1 = val1 && ~(col4 = val4) || ~(col5 = val5))
        // => (col1 = val1 || ~(col4 = val4)) && (col1 = val1 || ~(col5 = val5)).
        // However, we don't have this reduction now, so we have a filter in a suboptimized CNF.
        FilterTestUtils.or(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.not(FilterTestUtils.selector("col4", "val4")),
            FilterTestUtils.not(FilterTestUtils.selector("col5", "val5"))
        ),
        FilterTestUtils.or(
            FilterTestUtils.selector("col2", "val2"),
            FilterTestUtils.not(FilterTestUtils.selector("col4", "val4")),
            FilterTestUtils.not(FilterTestUtils.selector("col5", "val5"))
        )
    );
    final Filter cnf = Filters.toCnf(filter);
    assertFilter(filter, expected, cnf);
  }

  @Test
  public void testToNormalizedOrClausesWithComplexFilterIncludingNotAndOr()
  {
    final Filter filter = FilterTestUtils.and(
        FilterTestUtils.or(
            FilterTestUtils.and(
                FilterTestUtils.selector("col1", "val1"),
                FilterTestUtils.selector("col2", "val2")
            ),
            FilterTestUtils.not(
                FilterTestUtils.and(
                    FilterTestUtils.selector("col4", "val4"),
                    FilterTestUtils.selector("col5", "val5")
                )
            )
        ),
        FilterTestUtils.or(
            FilterTestUtils.not(
                FilterTestUtils.or(
                    FilterTestUtils.selector("col2", "val2"),
                    FilterTestUtils.selector("col4", "val4"),
                    FilterTestUtils.selector("col5", "val5")
                )
            ),
            FilterTestUtils.and(
                FilterTestUtils.selector("col1", "val1"),
                FilterTestUtils.selector("col3", "val3")
            )
        ),
        FilterTestUtils.and(
            FilterTestUtils.or(
                FilterTestUtils.selector("col1", "val1"),
                FilterTestUtils.selector("col2", "val22"), // selecting different value
                FilterTestUtils.selector("col3", "val3")
            ),
            FilterTestUtils.not(
                FilterTestUtils.selector("col1", "val11")
            )
        ),
        FilterTestUtils.and(
            FilterTestUtils.or(
                FilterTestUtils.selector("col1", "val1"),
                FilterTestUtils.selector("col2", "val22"),
                FilterTestUtils.selector("col3", "val3")
            ),
            FilterTestUtils.not(
                FilterTestUtils.selector("col1", "val11") // selecting different value
            )
        )
    );
    final Set<Filter> expected = ImmutableSet.of(
        FilterTestUtils.or(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.selector("col2", "val22"),
            FilterTestUtils.selector("col3", "val3")
        ),
        FilterTestUtils.or(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.not(FilterTestUtils.selector("col2", "val2"))
        ),
        FilterTestUtils.or(
            FilterTestUtils.not(FilterTestUtils.selector("col2", "val2")),
            FilterTestUtils.selector("col3", "val3")
        ),
        FilterTestUtils.or(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.not(FilterTestUtils.selector("col4", "val4"))
        ),
        FilterTestUtils.or(
            FilterTestUtils.selector("col3", "val3"),
            FilterTestUtils.not(FilterTestUtils.selector("col4", "val4"))
        ),
        FilterTestUtils.or(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.not(FilterTestUtils.selector("col5", "val5"))
        ),
        FilterTestUtils.or(
            FilterTestUtils.selector("col3", "val3"),
            FilterTestUtils.not(FilterTestUtils.selector("col5", "val5"))
        ),
        FilterTestUtils.not(FilterTestUtils.selector("col1", "val11")),
        // The below OR filter could be eliminated because this filter also has
        // (col1 = val1 || ~(col4 = val4)) && (col1 = val1 || ~(col5 = val5)).
        // The reduction process would be
        // (col1 = val1 || ~(col4 = val4)) && (col1 = val1 || ~(col5 = val5)) && (col1 = val1 || ~(col4 = val4) || ~(col5 = val5))
        // => (col1 = val1 && ~(col4 = val4) || ~(col5 = val5)) && (col1 = val1 || ~(col4 = val4) || ~(col5 = val5))
        // => (col1 = val1 && ~(col4 = val4) || ~(col5 = val5))
        // => (col1 = val1 || ~(col4 = val4)) && (col1 = val1 || ~(col5 = val5)).
        // However, we don't have this reduction now, so we have a filter in a suboptimized CNF.
        FilterTestUtils.or(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.not(FilterTestUtils.selector("col4", "val4")),
            FilterTestUtils.not(FilterTestUtils.selector("col5", "val5"))
        ),
        FilterTestUtils.or(
            FilterTestUtils.selector("col2", "val2"),
            FilterTestUtils.not(FilterTestUtils.selector("col4", "val4")),
            FilterTestUtils.not(FilterTestUtils.selector("col5", "val5"))
        )
    );
    final Set<Filter> normalizedOrClauses = Filters.toNormalizedOrClauses(filter);
    Assert.assertEquals(expected, normalizedOrClauses);
  }

  @Test
  public void testToCnfCollapsibleBigFilter()
  {
    Set<Filter> ands = new HashSet<>();
    Set<Filter> ors = new HashSet<>();
    for (int i = 0; i < 12; i++) {
      ands.add(
          FilterTestUtils.and(
              FilterTestUtils.selector("col3", "val3"),
              FilterTestUtils.selector("col4", "val4"),
              FilterTestUtils.selector("col5", StringUtils.format("val%d", i))
          )
      );
      ors.add(FilterTestUtils.selector("col5", StringUtils.format("val%d", i)));
    }

    final Filter bigFilter = FilterTestUtils.and(
        new OrFilter(ands),
        FilterTestUtils.selector("col1", "val1"),
        FilterTestUtils.selector("col2", "val2")
    );
    final Filter expectedCnf = FilterTestUtils.and(
        FilterTestUtils.selector("col1", "val1"),
        FilterTestUtils.selector("col2", "val2"),
        FilterTestUtils.selector("col3", "val3"),
        FilterTestUtils.selector("col4", "val4"),
        new OrFilter(ors)
    );
    final Filter cnf = Filters.toCnf(bigFilter);
    assertFilter(bigFilter, expectedCnf, cnf);
  }

  @Test
  public void testPullOrOnlyFilter()
  {
    final Filter filter = FilterTestUtils.or(
        FilterTestUtils.selector("col1", "val1"),
        FilterTestUtils.selector("col2", "val2"),
        FilterTestUtils.selector("col3", "val3")
    );

    assertFilter(filter, filter, CalciteCnfHelper.pull(filter));
  }

  @Test
  public void testPullNotPullableFilter()
  {
    final Filter filter = FilterTestUtils.or(
        FilterTestUtils.and(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.selector("col2", "val2")
        ),
        FilterTestUtils.and(
            FilterTestUtils.selector("col3", "val3"),
            FilterTestUtils.selector("col4", "val4")
        ),
        FilterTestUtils.and(
            FilterTestUtils.selector("col5", "val5"),
            FilterTestUtils.selector("col6", "val6")
        ),
        FilterTestUtils.selector("col7", "val7")
    );

    assertFilter(filter, filter, CalciteCnfHelper.pull(filter));
  }

  @Test
  public void testToCnfFilterThatPullCannotConvertToCnfProperly()
  {
    final Filter filter = FilterTestUtils.or(
        FilterTestUtils.and(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.selector("col2", "val2")
        ),
        FilterTestUtils.and(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.selector("col3", "val3"),
            FilterTestUtils.selector("col4", "val4")
        )
    );

    final Filter expectedCnf = FilterTestUtils.and(
        FilterTestUtils.selector("col1", "val1"),
        FilterTestUtils.or(
            FilterTestUtils.selector("col2", "val2"),
            FilterTestUtils.selector("col3", "val3")
        ),
        FilterTestUtils.or(
            FilterTestUtils.selector("col2", "val2"),
            FilterTestUtils.selector("col4", "val4")
        )
    );

    assertFilter(filter, expectedCnf, Filters.toCnf(filter));
  }

  @Test
  public void testToNormalizedOrClausesNonAndFilterShouldReturnSingleton()
  {
    Filter filter = FilterTestUtils.or(
        FilterTestUtils.selector("col1", "val1"),
        FilterTestUtils.selector("col2", "val2")
    );
    Set<Filter> expected = Collections.singleton(filter);
    Set<Filter> normalizedOrClauses = Filters.toNormalizedOrClauses(filter);
    Assert.assertEquals(expected, normalizedOrClauses);
  }

  @Test
  public void testTrueFalseFilterRequiredColumnRewrite()
  {
    Assert.assertTrue(TrueFilter.instance().supportsRequiredColumnRewrite());
    Assert.assertTrue(FalseFilter.instance().supportsRequiredColumnRewrite());

    Assert.assertEquals(TrueFilter.instance(), TrueFilter.instance().rewriteRequiredColumns(ImmutableMap.of()));
    Assert.assertEquals(FalseFilter.instance(), FalseFilter.instance().rewriteRequiredColumns(ImmutableMap.of()));
  }

  private void assertFilter(Filter original, Filter expectedConverted, Filter actualConverted)
  {
    assertEquivalent(original, expectedConverted);
    Assert.assertEquals(expectedConverted, actualConverted);
  }

  /**
   * Assert the given two filters are equivalent by comparing their truth table.
   */
  private void assertEquivalent(Filter f1, Filter f2)
  {
    final Set<SelectorFilter> s1 = searchForSelectors(f1);
    final Set<SelectorFilter> s2 = searchForSelectors(f2);
    Assert.assertEquals(s1, s2);

    // Compare truth table
    final List<SelectorFilter> selectorFilters = new ArrayList<>(s1);
    List<Map<SelectorFilter, Boolean>> truthValues = populate(selectorFilters, selectorFilters.size() - 1);
    for (Map<SelectorFilter, Boolean> truthValue : truthValues) {
      Assert.assertEquals(evaluateFilterWith(f1, truthValue), evaluateFilterWith(f2, truthValue));
    }
  }

  /**
   * Recursively populate all permutations of truth values.
   *
   * @param selectorFilters selector filters
   * @param cursor          the offset to the current selectFilter to pupulate truth values
   *
   * @return a list of truth values populated up to the cursor
   */
  private List<Map<SelectorFilter, Boolean>> populate(List<SelectorFilter> selectorFilters, int cursor)
  {
    // populateFalse and populateTrue will include the false and the true value
    // for the current selectFilter, respectively.
    final List<Map<SelectorFilter, Boolean>> populateFalse;
    final List<Map<SelectorFilter, Boolean>> populateTrue;
    if (cursor == 0) {
      Map<SelectorFilter, Boolean> mapForFalse = new HashMap<>();
      Map<SelectorFilter, Boolean> mapForTrue = new HashMap<>();
      for (SelectorFilter eachFilter : selectorFilters) {
        mapForFalse.put(eachFilter, false);
        mapForTrue.put(eachFilter, false);
      }
      populateFalse = new ArrayList<>();
      populateFalse.add(mapForFalse);
      populateTrue = new ArrayList<>();
      populateTrue.add(mapForTrue);
    } else {
      final List<Map<SelectorFilter, Boolean>> populated = populate(selectorFilters, cursor - 1);
      populateFalse = new ArrayList<>(populated.size());
      populateTrue = new ArrayList<>(populated.size());
      for (Map<SelectorFilter, Boolean> eachMap : populated) {
        populateFalse.add(new HashMap<>(eachMap));
        populateTrue.add(new HashMap<>(eachMap));
      }
    }

    for (Map<SelectorFilter, Boolean> eachMap : populateTrue) {
      eachMap.put(selectorFilters.get(cursor), true);
    }

    final List<Map<SelectorFilter, Boolean>> allPopulated = new ArrayList<>(populateFalse);
    allPopulated.addAll(populateTrue);
    return allPopulated;
  }

  private Set<SelectorFilter> searchForSelectors(Filter filter)
  {
    Set<SelectorFilter> found = new HashSet<>();
    visitSelectorFilters(filter, selectorFilter -> {
      found.add(selectorFilter);
      return selectorFilter;
    });
    return found;
  }

  private boolean evaluateFilterWith(Filter filter, Map<SelectorFilter, Boolean> values)
  {
    Filter rewrittenFilter = visitSelectorFilters(filter, selectorFilter -> {
      Boolean truth = values.get(selectorFilter);
      if (truth == null) {
        throw new ISE("Can't find truth value for selectorFilter[%s]", selectorFilter);
      }
      return truth ? TrueFilter.instance() : FalseFilter.instance();
    });
    return rewrittenFilter.makeMatcher(
        new ColumnSelectorFactory()
        {
          @Override
          public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
          {
            return null;
          }

          @Override
          public ColumnValueSelector makeColumnValueSelector(String columnName)
          {
            return null;
          }

          @Nullable
          @Override
          public ColumnCapabilities getColumnCapabilities(String column)
          {
            return null;
          }
        }
    ).matches();
  }

  private Filter visitSelectorFilters(Filter filter, Function<SelectorFilter, Filter> visitAction)
  {
    if (filter instanceof AndFilter) {
      Set<Filter> newChildren = new HashSet<>();
      for (Filter child : ((AndFilter) filter).getFilters()) {
        newChildren.add(visitSelectorFilters(child, visitAction));
      }
      return new AndFilter(newChildren);
    } else if (filter instanceof OrFilter) {
      Set<Filter> newChildren = new HashSet<>();
      for (Filter child : ((OrFilter) filter).getFilters()) {
        newChildren.add(visitSelectorFilters(child, visitAction));
      }
      return new OrFilter(newChildren);
    } else if (filter instanceof NotFilter) {
      Filter child = ((NotFilter) filter).getBaseFilter();
      return new NotFilter(visitSelectorFilters(child, visitAction));
    } else if (filter instanceof SelectorFilter) {
      return visitAction.apply((SelectorFilter) filter);
    }
    return filter;
  }
}
