package io.druid.query.filter;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import io.druid.js.JavaScriptConfig;
import io.druid.query.extraction.IdentityExtractionFn;
import io.druid.query.search.search.ContainsSearchQuerySpec;
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
  private final DimFilter bound1 = new BoundDimFilter("dim1", "from", "to", false, false, false, null);
  private final DimFilter bound2 = new BoundDimFilter("dim1", null, "tillend", false, false, false, null);
  private final DimFilter bound3 = new BoundDimFilter("dim1", "notincluded", null, true, false, false, null);
  private final DimFilter bound4 = new BoundDimFilter("dim2", "again", "exclusive", true, true, false, null);
  private final DimFilter other1 = new RegexDimFilter("someDim", "pattern", null);
  private final DimFilter other2 = new JavaScriptDimFilter("someOtherDim", "function(x) { return x }", null,
                                                           JavaScriptConfig.getDefault());
  private final DimFilter other3 = new SearchQueryDimFilter("dim", new ContainsSearchQuerySpec("a", true), null);

  private static final RangeSet all = rangeSet(ImmutableList.of(Range.<String>all()));
  private static final RangeSet empty = rangeSet(ImmutableList.<Range<String>>of());

  @Test
  public void testSimpleFilter () {
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
  }

  @Test
  public void testAndFilter () {
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
  public void testOrFilter () {
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
  }

  public void testNotFilter () {
    DimFilter not1 = new NotDimFilter(selector1);
    RangeSet expected1 = rangeSet(ImmutableList.of(Range.lessThan("a"), Range.greaterThan("a")));
    Assert.assertEquals(expected1, not1.getDimensionRangeSet("dim1"));

  }

  public void testCombinedFilter () {

  }

  private static Range<String> point(String s) {
    return Range.singleton(s);
  }

  private static RangeSet<String> rangeSet (Range<String> ranges) {
    return ImmutableRangeSet.of(ranges);
  }

  private static RangeSet<String> rangeSet (List<Range<String>> ranges) {
    ImmutableRangeSet.Builder<String> builder = ImmutableRangeSet.builder();
    for (Range<String> range : ranges) {
      builder.add(range);
    }
    return builder.build();
  }

}
