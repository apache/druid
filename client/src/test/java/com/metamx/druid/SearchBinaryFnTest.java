package com.metamx.druid;

import com.google.common.collect.ImmutableList;
import com.metamx.druid.query.search.LexicographicSearchSortSpec;
import com.metamx.druid.query.search.SearchHit;
import com.metamx.druid.query.search.StrlenSearchSortSpec;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.SearchResultValue;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Iterator;

/**
 */
public class SearchBinaryFnTest
{
  private final DateTime currTime = new DateTime();

  private void assertSearchMergeResult(Object o1, Object o2)
  {
    Iterator i1 = ((Iterable) o1).iterator();
    Iterator i2 = ((Iterable) o2).iterator();
    while (i1.hasNext() && i2.hasNext()) {
      Assert.assertEquals(i1.next(), i2.next());
    }
    Assert.assertTrue(!i1.hasNext() && !i2.hasNext());
  }

  @Test
  public void testMerge()
  {
    Result<SearchResultValue> r1 = new Result<SearchResultValue>(
        currTime,
        new SearchResultValue(
            ImmutableList.<SearchHit>of(
                new SearchHit(
                    "blah",
                    "foo"
                )
            )
        )
    );

    Result<SearchResultValue> r2 = new Result<SearchResultValue>(
        currTime,
        new SearchResultValue(
            ImmutableList.<SearchHit>of(
                new SearchHit(
                    "blah2",
                    "foo2"
                )
            )
        )
    );

    Result<SearchResultValue> expected = new Result<SearchResultValue>(
        currTime,
        new SearchResultValue(
            ImmutableList.<SearchHit>of(
                new SearchHit(
                    "blah",
                    "foo"
                ),
                new SearchHit(
                    "blah2",
                    "foo2"
                )
            )
        )
    );

    Result<SearchResultValue> actual = new SearchBinaryFn(new LexicographicSearchSortSpec(), QueryGranularity.ALL).apply(r1, r2);
    Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());
    assertSearchMergeResult(expected.getValue(), actual.getValue());
  }

  @Test
  public void testMergeDay()
  {
    Result<SearchResultValue> r1 = new Result<SearchResultValue>(
        currTime,
        new SearchResultValue(
            ImmutableList.<SearchHit>of(
                new SearchHit(
                    "blah",
                    "foo"
                )
            )
        )
    );

    Result<SearchResultValue> r2 = new Result<SearchResultValue>(
        currTime,
        new SearchResultValue(
            ImmutableList.<SearchHit>of(
                new SearchHit(
                    "blah2",
                    "foo2"
                )
            )
        )
    );

    Result<SearchResultValue> expected = new Result<SearchResultValue>(
        new DateTime(QueryGranularity.DAY.truncate(currTime.getMillis())),
        new SearchResultValue(
            ImmutableList.<SearchHit>of(
                new SearchHit(
                    "blah",
                    "foo"
                ),
                new SearchHit(
                    "blah2",
                    "foo2"
                )
            )
        )
    );

    Result<SearchResultValue> actual = new SearchBinaryFn(new LexicographicSearchSortSpec(), QueryGranularity.DAY).apply(r1, r2);
    Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());
    assertSearchMergeResult(expected.getValue(), actual.getValue());
  }

  @Test
  public void testMergeOneResultNull()
  {
    Result<SearchResultValue> r1 = new Result<SearchResultValue>(
        currTime,
        new SearchResultValue(
            ImmutableList.<SearchHit>of(
                new SearchHit(
                    "blah",
                    "foo"
                )
            )
        )
    );

    Result<SearchResultValue> r2 = null;

    Result<SearchResultValue> expected = r1;

    Result<SearchResultValue> actual = new SearchBinaryFn(new LexicographicSearchSortSpec(), QueryGranularity.ALL).apply(r1, r2);
    Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());
    assertSearchMergeResult(expected.getValue(), actual.getValue());
  }

  @Test
  public void testMergeShiftedTimestamp()
  {
    Result<SearchResultValue> r1 = new Result<SearchResultValue>(
        currTime,
        new SearchResultValue(
            ImmutableList.<SearchHit>of(
                new SearchHit(
                    "blah",
                    "foo"
                )
            )
        )
    );

    Result<SearchResultValue> r2 = new Result<SearchResultValue>(
        currTime.plusHours(2),
        new SearchResultValue(
            ImmutableList.<SearchHit>of(
                new SearchHit(
                    "blah2",
                    "foo2"
                )
            )
        )
    );

    Result<SearchResultValue> expected = new Result<SearchResultValue>(
        currTime,
        new SearchResultValue(
            ImmutableList.<SearchHit>of(
                new SearchHit(
                    "blah",
                    "foo"
                ),
                new SearchHit(
                    "blah2",
                    "foo2"
                )
            )
        )
    );

    Result<SearchResultValue> actual = new SearchBinaryFn(new LexicographicSearchSortSpec(), QueryGranularity.ALL).apply(r1, r2);
    Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());
    assertSearchMergeResult(expected.getValue(), actual.getValue());
  }

  @Test
  public void testStrlenMerge()
  {
    Result<SearchResultValue> r1 = new Result<SearchResultValue>(
        currTime,
        new SearchResultValue(
            ImmutableList.<SearchHit>of(
                new SearchHit(
                    "blah",
                    "thisislong"
                )
            )
        )
    );

    Result<SearchResultValue> r2 = new Result<SearchResultValue>(
        currTime,
        new SearchResultValue(
            ImmutableList.<SearchHit>of(
                new SearchHit(
                    "blah",
                    "short"
                )
            )
        )
    );

    Result<SearchResultValue> expected = new Result<SearchResultValue>(
        currTime,
        new SearchResultValue(
            ImmutableList.<SearchHit>of(
                new SearchHit(
                    "blah",
                    "short"
                ),
                new SearchHit(
                    "blah",
                    "thisislong"
                )
            )
        )
    );

    Result<SearchResultValue> actual = new SearchBinaryFn(new StrlenSearchSortSpec(), QueryGranularity.ALL).apply(r1, r2);
    Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());
    assertSearchMergeResult(expected.getValue(), actual.getValue());
  }

  @Test
  public void testMergeUniqueResults()
  {
    Result<SearchResultValue> r1 = new Result<SearchResultValue>(
        currTime,
        new SearchResultValue(
            ImmutableList.<SearchHit>of(
                new SearchHit(
                    "blah",
                    "foo"
                )
            )
        )
    );

    Result<SearchResultValue> r2 = r1;

    Result<SearchResultValue> expected = r1;

    Result<SearchResultValue> actual = new SearchBinaryFn(new LexicographicSearchSortSpec(), QueryGranularity.ALL).apply(r1, r2);
    Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());
    assertSearchMergeResult(expected.getValue(), actual.getValue());
  }
}
