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

package io.druid.query.filter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import io.druid.js.JavaScriptConfig;
import io.druid.query.extraction.IdentityExtractionFn;
import io.druid.query.ordering.StringComparators;
import io.druid.query.search.search.ContainsSearchQuerySpec;
import io.druid.segment.column.Column;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class GetDimensionRangeSetTest
{

  private final DimFilter selector1 = new SelectorDimFilter("dim1", "a", null);
  private final DimFilter selector2 = new SelectorDimFilter("dim1", "z", null);
  private final DimFilter selector3 = new SelectorDimFilter("dim2", "c", null);
  private final DimFilter selector4 = new SelectorDimFilter("dimWorld", "find", IdentityExtractionFn.getInstance());
  private final DimFilter selector5 = new SelectorDimFilter("dim1", null, null);
  private final DimFilter in1 = new InDimFilter("dim1", ImmutableList.of("testing", "this", "filter", "tillend"), null);
  private final DimFilter in2 = new InDimFilter("dim2", ImmutableList.of("again"), null);
  private final DimFilter in3 = new InDimFilter("dim1", Arrays.asList("null", null), null);
  private final DimFilter bound1 = new BoundDimFilter("dim1", "from", "to", false, false, false, null,
                                                      StringComparators.LEXICOGRAPHIC
  );
  private final DimFilter bound2 = new BoundDimFilter("dim1", null, "tillend", false, false, false, null,
                                                      StringComparators.LEXICOGRAPHIC
  );
  private final DimFilter bound3 = new BoundDimFilter("dim1", "notincluded", null, true, false, false, null,
                                                      StringComparators.LEXICOGRAPHIC
  );
  private final DimFilter bound4 = new BoundDimFilter("dim2", "again", "exclusive", true, true, false, null,
                                                      StringComparators.LEXICOGRAPHIC
  );
  private final DimFilter other1 = new RegexDimFilter("someDim", "pattern", null);
  private final DimFilter other2 = new JavaScriptDimFilter("someOtherDim", "function(x) { return x }", null,
                                                           JavaScriptConfig.getEnabledInstance());
  private final DimFilter other3 = new SearchQueryDimFilter("dim", new ContainsSearchQuerySpec("a", true), null);

  private final DimFilter interval1 = new IntervalDimFilter(
      Column.TIME_COLUMN_NAME,
      Arrays.asList(
          Interval.parse("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.004Z"),
          Interval.parse("1975-01-01T00:00:00.001Z/1980-01-01T00:00:00.004Z")
      ),
      null
  );

  private final DimFilter interval2 = new IntervalDimFilter(
      "dim1",
      Arrays.asList(
          Interval.parse("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.004Z"),
          Interval.parse("1975-01-01T00:00:00.001Z/1980-01-01T00:00:00.004Z")
      ),
      null
  );

  private static final RangeSet all = rangeSet(ImmutableList.of(Range.<String>all()));
  private static final RangeSet empty = rangeSet(ImmutableList.<Range<String>>of());

  @Test
  public void testSimpleFilter()
  {
    RangeSet expected1 = rangeSet(point("a"));
    Assert.assertEquals(expected1, selector1.getDimensionRangeSet("dim1"));
    Assert.assertNull(selector1.getDimensionRangeSet("dim2"));

    RangeSet expected2 = rangeSet(point(""));
    Assert.assertEquals(expected2, selector5.getDimensionRangeSet("dim1"));

    RangeSet expected3 = rangeSet(ImmutableList.of(point("testing"), point("this"), point("filter"), point("tillend")));
    Assert.assertEquals(expected3, in1.getDimensionRangeSet("dim1"));

    RangeSet expected4 = rangeSet(ImmutableList.of(point("null"), point("")));
    Assert.assertEquals(expected4, in3.getDimensionRangeSet("dim1"));

    RangeSet expected5 = ImmutableRangeSet.of(Range.closed("from", "to"));
    Assert.assertEquals(expected5, bound1.getDimensionRangeSet("dim1"));

    RangeSet expected6 = ImmutableRangeSet.of(Range.atMost("tillend"));
    Assert.assertEquals(expected6, bound2.getDimensionRangeSet("dim1"));

    RangeSet expected7 = ImmutableRangeSet.of(Range.greaterThan("notincluded"));
    Assert.assertEquals(expected7, bound3.getDimensionRangeSet("dim1"));

    Assert.assertNull(other1.getDimensionRangeSet("someDim"));
    Assert.assertNull(other2.getDimensionRangeSet("someOtherDim"));
    Assert.assertNull(other3.getDimensionRangeSet("dim"));

    Assert.assertNull(interval1.getDimensionRangeSet(Column.TIME_COLUMN_NAME));
    Assert.assertNull(interval2.getDimensionRangeSet("dim1"));
  }

  @Test
  public void testAndFilter()
  {
    DimFilter and1 = new AndDimFilter(ImmutableList.of(selector1, selector2, in1));
    Assert.assertEquals(empty, and1.getDimensionRangeSet("dim1"));
    Assert.assertNull(and1.getDimensionRangeSet("dim2"));

    DimFilter and2 = new AndDimFilter(ImmutableList.of(selector3, bound1, other1));
    RangeSet expected2 = rangeSet(ImmutableList.of(Range.closed("from", "to")));
    Assert.assertEquals(expected2, and2.getDimensionRangeSet("dim1"));

    DimFilter and3 = new AndDimFilter(ImmutableList.of(in2, bound1, bound2, bound3, bound4));
    RangeSet expected3 = rangeSet(Range.openClosed("notincluded", "tillend"));
    Assert.assertEquals(expected3, and3.getDimensionRangeSet("dim1"));
    Assert.assertEquals(empty, and3.getDimensionRangeSet("dim2"));

    DimFilter and4 = new AndDimFilter(ImmutableList.of(in3, bound3));
    RangeSet expected4 = rangeSet(point("null"));
    Assert.assertEquals(expected4, and4.getDimensionRangeSet("dim1"));

    DimFilter and5 = new AndDimFilter(ImmutableList.of(and3, in1));
    RangeSet expected5 = rangeSet(ImmutableList.of(point("testing"), point("this"), point("tillend")));
    Assert.assertEquals(expected5, and5.getDimensionRangeSet("dim1"));
  }

  @Test
  public void testOrFilter()
  {
    DimFilter or1 = new OrDimFilter(ImmutableList.of(selector1, selector2, selector5));
    RangeSet expected1 = rangeSet(ImmutableList.of(point(""), point("a"), point("z")));
    Assert.assertEquals(expected1, or1.getDimensionRangeSet("dim1"));

    DimFilter or2 = new OrDimFilter(ImmutableList.of(selector5, in1, in3));
    RangeSet expected2 = rangeSet(ImmutableList.of(point("testing"), point("this"), point("filter"), point("tillend"),
                                                   point("null"), point("")));
    Assert.assertEquals(expected2, or2.getDimensionRangeSet("dim1"));

    DimFilter or3 = new OrDimFilter(ImmutableList.of(bound1, bound2, bound3));
    Assert.assertEquals(all, or3.getDimensionRangeSet("dim1"));

    DimFilter or4 = new OrDimFilter(ImmutableList.of(selector1, selector2, selector3, selector4, selector5));
    Assert.assertNull(or4.getDimensionRangeSet("dim1"));
    Assert.assertNull(or4.getDimensionRangeSet("dim2"));

    DimFilter or5 = new OrDimFilter(ImmutableList.of(or1, or2, bound1));
    RangeSet expected5 = rangeSet(ImmutableList.of(point(""), point("a"), point("filter"), Range.closed("from", "to"),
                                                   point("z")));
    Assert.assertEquals(expected5, or5.getDimensionRangeSet("dim1"));
  }

  @Test
  public void testNotFilter()
  {
    DimFilter not1 = new NotDimFilter(selector1);
    RangeSet expected1 = rangeSet(ImmutableList.of(Range.lessThan("a"), Range.greaterThan("a")));
    Assert.assertEquals(expected1, not1.getDimensionRangeSet("dim1"));
    Assert.assertNull(not1.getDimensionRangeSet("dim2"));

    DimFilter not2 = new NotDimFilter(in3);
    RangeSet expected2 = rangeSet(ImmutableList.of(Range.lessThan(""), Range.open("", "null"), Range.greaterThan("null")));
    Assert.assertEquals(expected2, not2.getDimensionRangeSet("dim1"));

    DimFilter not3 = new NotDimFilter(bound1);
    RangeSet expected3 = rangeSet(ImmutableList.of(Range.lessThan("from"), Range.greaterThan("to")));
    Assert.assertEquals(expected3, not3.getDimensionRangeSet("dim1"));

    DimFilter not4 = new NotDimFilter(not2);
    RangeSet expected4 = rangeSet(ImmutableList.of(point(""), point("null")));
    Assert.assertEquals(expected4, not4.getDimensionRangeSet("dim1"));

    DimFilter or1 = new OrDimFilter(ImmutableList.of(selector1, selector2, bound1, bound3));
    DimFilter not5 = new NotDimFilter(or1);
    RangeSet expected5 = rangeSet(ImmutableList.of(Range.lessThan("a"), Range.open("a", "from")));
    Assert.assertEquals(expected5, not5.getDimensionRangeSet("dim1"));
    Assert.assertNull(not5.getDimensionRangeSet("dim2"));

    DimFilter or2 = new OrDimFilter(ImmutableList.of(selector3, in3, bound2, bound4, other3));
    DimFilter not6 = new NotDimFilter(or2);
    RangeSet expected6a = rangeSet(ImmutableList.of(Range.greaterThan("tillend")));
    RangeSet expected6b = rangeSet(ImmutableList.of(Range.atMost("again"), Range.atLeast("exclusive")));
    Assert.assertEquals(expected6a, not6.getDimensionRangeSet("dim1"));
    Assert.assertEquals(expected6b, not6.getDimensionRangeSet("dim2"));

    DimFilter and1 = new AndDimFilter(ImmutableList.of(in1, bound1, bound2));
    DimFilter not7 = new NotDimFilter(and1);
    RangeSet expected7 = rangeSet(ImmutableList.of(Range.lessThan("testing"), Range.open("testing", "this"),
                                                   Range.open("this", "tillend"), Range.greaterThan("tillend")));
    Assert.assertEquals(expected7, not7.getDimensionRangeSet("dim1"));
    Assert.assertNull(not7.getDimensionRangeSet("dim2"));

    DimFilter and2 = new AndDimFilter(ImmutableList.of(bound1, bound2, bound3, bound4));
    DimFilter not8 = new NotDimFilter(and2);
    Assert.assertNull(not8.getDimensionRangeSet("dim1"));
    Assert.assertNull(not8.getDimensionRangeSet("dim2"));

  }

  private static Range<String> point(String s)
  {
    return Range.singleton(s);
  }

  private static RangeSet<String> rangeSet(Range<String> ranges)
  {
    return ImmutableRangeSet.of(ranges);
  }

  private static RangeSet<String> rangeSet(List<Range<String>> ranges)
  {
    ImmutableRangeSet.Builder<String> builder = ImmutableRangeSet.builder();
    for (Range<String> range : ranges) {
      builder.add(range);
    }
    return builder.build();
  }

}
