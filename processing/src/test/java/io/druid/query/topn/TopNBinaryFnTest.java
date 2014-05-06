/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.topn;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.granularity.QueryGranularity;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class TopNBinaryFnTest
{
  final CountAggregatorFactory rowsCount = new CountAggregatorFactory("rows");
  final LongSumAggregatorFactory indexLongSum = new LongSumAggregatorFactory("index", "index");
  final ConstantPostAggregator constant = new ConstantPostAggregator("const", 1L, null);
  final FieldAccessPostAggregator rowsPostAgg = new FieldAccessPostAggregator("rows", "rows");
  final FieldAccessPostAggregator indexPostAgg = new FieldAccessPostAggregator("index", "index");
  final ArithmeticPostAggregator addrowsindexconstant = new ArithmeticPostAggregator(
      "addrowsindexconstant",
      "+",
      Lists.newArrayList(constant, rowsPostAgg, indexPostAgg)
  );
  final List<AggregatorFactory> aggregatorFactories = Arrays.asList(
      rowsCount,
      indexLongSum
  );
  final List<PostAggregator> postAggregators = Arrays.<PostAggregator>asList(
      addrowsindexconstant
  );
  private final DateTime currTime = new DateTime();

  private void assertTopNMergeResult(Object o1, Object o2)
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
    Result<TopNResultValue> result1 = new Result<TopNResultValue>(
        currTime,
        new TopNResultValue(
            ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(
                    "rows", 1L,
                    "index", 2L,
                    "testdim", "1"
                ),
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 4L,
                    "testdim", "2"
                ),
                ImmutableMap.<String, Object>of(
                    "rows", 0L,
                    "index", 2L,
                    "testdim", "3"
                )
            )
        )
    );
    Result<TopNResultValue> result2 = new Result<TopNResultValue>(
        currTime,
        new TopNResultValue(
            ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 3L,
                    "testdim", "1"
                ),
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 0L,
                    "testdim", "2"
                ),
                ImmutableMap.<String, Object>of(
                    "rows", 0L,
                    "index", 1L,
                    "testdim", "3"
                )
            )
        )
    );

    Result<TopNResultValue> expected = new Result<TopNResultValue>(
        currTime,
        new TopNResultValue(
            ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(
                    "testdim", "1",
                    "rows", 3L,
                    "index", 5L
                ),

                ImmutableMap.<String, Object>of(
                    "testdim", "2",
                    "rows", 4L,
                    "index", 4L
                )
            )
        )
    );

    Result<TopNResultValue> actual = new TopNBinaryFn(
        TopNResultMerger.identity,
        QueryGranularity.ALL,
        new DefaultDimensionSpec("testdim", null),
        new NumericTopNMetricSpec("index"),
        2,
        aggregatorFactories,
        postAggregators
    ).apply(
        result1,
        result2
    );
    Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());
    assertTopNMergeResult(expected.getValue(), actual.getValue());
  }

  @Test
  public void testMergeDay()
  {
    Result<TopNResultValue> result1 = new Result<TopNResultValue>(
        currTime,
        new TopNResultValue(
            ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(
                    "rows", 1L,
                    "index", 2L,
                    "testdim", "1"
                ),
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 4L,
                    "testdim", "2"
                ),
                ImmutableMap.<String, Object>of(
                    "rows", 0L,
                    "index", 2L,
                    "testdim", "3"
                )
            )
        )
    );
    Result<TopNResultValue> result2 = new Result<TopNResultValue>(
        currTime,
        new TopNResultValue(
            ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 3L,
                    "testdim", "1"
                ),
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 0L,
                    "testdim", "2"
                ),
                ImmutableMap.<String, Object>of(
                    "rows", 0L,
                    "index", 1L,
                    "testdim", "3"
                )
            )
        )
    );

    Result<TopNResultValue> expected = new Result<TopNResultValue>(
        new DateTime(QueryGranularity.DAY.truncate(currTime.getMillis())),
        new TopNResultValue(
            ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(
                    "testdim", "1",
                    "rows", 3L,
                    "index", 5L
                ),
                ImmutableMap.<String, Object>of(
                    "testdim", "2",
                    "rows", 4L,
                    "index", 4L
                )
            )
        )
    );

    Result<TopNResultValue> actual = new TopNBinaryFn(
        TopNResultMerger.identity,
        QueryGranularity.DAY,
        new DefaultDimensionSpec("testdim", null),
        new NumericTopNMetricSpec("index"),
        2,
        aggregatorFactories,
        postAggregators
    ).apply(
        result1,
        result2
    );
    Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());
    assertTopNMergeResult(expected.getValue(), actual.getValue());
  }

  @Test
  public void testMergeOneResultNull()
  {
    Result<TopNResultValue> result1 = new Result<TopNResultValue>(
        currTime,
        new TopNResultValue(
            ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(
                    "rows", 1L,
                    "index", 2L,
                    "testdim", "1"
                ),
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 4L,
                    "testdim", "2"
                ),
                ImmutableMap.<String, Object>of(
                    "rows", 0L,
                    "index", 2L,
                    "testdim", "3"
                )
            )
        )
    );
    Result<TopNResultValue> result2 = null;

    Result<TopNResultValue> expected = result1;

    Result<TopNResultValue> actual = new TopNBinaryFn(
        TopNResultMerger.identity,
        QueryGranularity.ALL,
        new DefaultDimensionSpec("testdim", null),
        new NumericTopNMetricSpec("index"),
        2,
        aggregatorFactories,
        postAggregators
    ).apply(
        result1,
        result2
    );
    Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());
    assertTopNMergeResult(expected.getValue(), actual.getValue());
  }

  @Test
  public void testMergeByPostAgg()
  {
    Result<TopNResultValue> result1 = new Result<TopNResultValue>(
        currTime,
        new TopNResultValue(
            ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(
                    "rows", 1L,
                    "index", 2L,
                    "testdim", "1",
                    "addrowsindexconstant", 3.0
                ),
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 4L,
                    "testdim", "2",
                    "addrowsindexconstant", 7.0
                ),
                ImmutableMap.<String, Object>of(
                    "rows", 0L,
                    "index", 2L,
                    "testdim", "3",
                    "addrowsindexconstant", 3.0
                )
            )
        )
    );
    Result<TopNResultValue> result2 = new Result<TopNResultValue>(
        currTime,
        new TopNResultValue(
            ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 3L,
                    "testdim", "1",
                    "addrowsindexconstant", 6.0
                ),
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 0L,
                    "testdim", "2",
                    "addrowsindexconstant", 3.0
                ),
                ImmutableMap.<String, Object>of(
                    "rows", 4L,
                    "index", 5L,
                    "testdim", "other",
                    "addrowsindexconstant", 10.0
                )
            )
        )
    );

    Result<TopNResultValue> expected = new Result<TopNResultValue>(
        currTime,
        new TopNResultValue(
            ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(
                    "testdim", "other",
                    "rows", 4L,
                    "index", 5L,
                    "addrowsindexconstant", 10.0
                ),
                ImmutableMap.<String, Object>of(
                    "testdim", "1",
                    "rows", 3L,
                    "index", 5L,
                    "addrowsindexconstant", 9.0
                ),
                ImmutableMap.<String, Object>of(
                    "testdim", "2",
                    "rows", 4L,
                    "index", 4L,
                    "addrowsindexconstant", 9.0
                )
            )
        )
    );

    Result<TopNResultValue> actual = new TopNBinaryFn(
        TopNResultMerger.identity,
        QueryGranularity.ALL,
        new DefaultDimensionSpec("testdim", null),
        new NumericTopNMetricSpec("addrowsindexconstant"),
        3,
        aggregatorFactories,
        postAggregators
    ).apply(
        result1,
        result2
    );
    Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());
    assertTopNMergeResult(expected.getValue(), actual.getValue());
  }

  @Test
  public void testMergeShiftedTimestamp()
  {
    Result<TopNResultValue> result1 = new Result<TopNResultValue>(
        currTime,
        new TopNResultValue(
            ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(
                    "rows", 1L,
                    "index", 2L,
                    "testdim", "1"
                ),
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 4L,
                    "testdim", "2"
                ),
                ImmutableMap.<String, Object>of(
                    "rows", 0L,
                    "index", 2L,
                    "testdim", "3"
                )
            )
        )
    );
    Result<TopNResultValue> result2 = new Result<TopNResultValue>(
        currTime.plusHours(2),
        new TopNResultValue(
            ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 3L,
                    "testdim", "1"
                ),
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 0L,
                    "testdim", "2"
                ),
                ImmutableMap.<String, Object>of(
                    "rows", 0L,
                    "index", 1L,
                    "testdim", "3"
                )
            )
        )
    );

    Result<TopNResultValue> expected = new Result<TopNResultValue>(
        currTime,
        new TopNResultValue(
            ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(
                    "testdim", "1",
                    "rows", 3L,
                    "index", 5L
                ),
                ImmutableMap.<String, Object>of(
                    "testdim", "2",
                    "rows", 4L,
                    "index", 4L
                )
            )
        )
    );

    Result<TopNResultValue> actual = new TopNBinaryFn(
        TopNResultMerger.identity,
        QueryGranularity.ALL,
        new DefaultDimensionSpec("testdim", null),
        new NumericTopNMetricSpec("index"),
        2,
        aggregatorFactories,
        postAggregators
    ).apply(
        result1,
        result2
    );
    Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());
    assertTopNMergeResult(expected.getValue(), actual.getValue());
  }
}
