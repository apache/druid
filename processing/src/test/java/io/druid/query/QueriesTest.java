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

package io.druid.query;

import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 */
public class QueriesTest
{
  @Test
  public void testVerifyAggregations() throws Exception
  {
    List<AggregatorFactory> aggFactories = Arrays.<AggregatorFactory>asList(
        new CountAggregatorFactory("count"),
        new DoubleSumAggregatorFactory("idx", "index"),
        new DoubleSumAggregatorFactory("rev", "revenue")
    );

    List<PostAggregator> postAggs = Arrays.<PostAggregator>asList(
        new ArithmeticPostAggregator(
            "addStuff",
            "+",
            Arrays.<PostAggregator>asList(
                new FieldAccessPostAggregator("idx", "idx"),
                new FieldAccessPostAggregator("count", "count")
            )
        )
    );

    boolean exceptionOccured = false;

    try {
      Queries.verifyAggregations(aggFactories, postAggs);
    }
    catch (IllegalArgumentException e) {
      exceptionOccured = true;
    }

    Assert.assertFalse(exceptionOccured);
  }

  @Test
  public void testVerifyAggregationsMissingVal() throws Exception
  {
    List<AggregatorFactory> aggFactories = Arrays.<AggregatorFactory>asList(
        new CountAggregatorFactory("count"),
        new DoubleSumAggregatorFactory("idx", "index"),
        new DoubleSumAggregatorFactory("rev", "revenue")
    );

    List<PostAggregator> postAggs = Arrays.<PostAggregator>asList(
        new ArithmeticPostAggregator(
            "addStuff",
            "+",
            Arrays.<PostAggregator>asList(
                new FieldAccessPostAggregator("idx", "idx2"),
                new FieldAccessPostAggregator("count", "count")
            )
        )
    );

    boolean exceptionOccured = false;

    try {
      Queries.verifyAggregations(aggFactories, postAggs);
    }
    catch (IllegalArgumentException e) {
      exceptionOccured = true;
    }

    Assert.assertTrue(exceptionOccured);
  }

  @Test
  public void testVerifyAggregationsMultiLevel() throws Exception
  {
    List<AggregatorFactory> aggFactories = Arrays.<AggregatorFactory>asList(
        new CountAggregatorFactory("count"),
        new DoubleSumAggregatorFactory("idx", "index"),
        new DoubleSumAggregatorFactory("rev", "revenue")
    );

    List<PostAggregator> postAggs = Arrays.<PostAggregator>asList(
        new ArithmeticPostAggregator(
            "divideStuff",
            "/",
            Arrays.<PostAggregator>asList(
                new ArithmeticPostAggregator(
                    "addStuff",
                    "+",
                    Arrays.asList(
                        new FieldAccessPostAggregator("idx", "idx"),
                        new ConstantPostAggregator("const", 1, null)
                    )
                ),
                new ArithmeticPostAggregator(
                    "subtractStuff",
                    "-",
                    Arrays.asList(
                        new FieldAccessPostAggregator("rev", "rev"),
                        new ConstantPostAggregator("const", 1, null)
                    )
                )
            )
        ),
        new ArithmeticPostAggregator(
            "addStuff",
            "+",
            Arrays.<PostAggregator>asList(
                new FieldAccessPostAggregator("divideStuff", "divideStuff"),
                new FieldAccessPostAggregator("count", "count")
            )
        )
    );

    boolean exceptionOccured = false;

    try {
      Queries.verifyAggregations(aggFactories, postAggs);
    }
    catch (IllegalArgumentException e) {
      exceptionOccured = true;
    }

    Assert.assertFalse(exceptionOccured);
  }

  @Test
  public void testVerifyAggregationsMultiLevelMissingVal() throws Exception
  {
    List<AggregatorFactory> aggFactories = Arrays.<AggregatorFactory>asList(
        new CountAggregatorFactory("count"),
        new DoubleSumAggregatorFactory("idx", "index"),
        new DoubleSumAggregatorFactory("rev", "revenue")
    );

    List<PostAggregator> postAggs = Arrays.<PostAggregator>asList(
        new ArithmeticPostAggregator(
            "divideStuff",
            "/",
            Arrays.<PostAggregator>asList(
                new ArithmeticPostAggregator(
                    "addStuff",
                    "+",
                    Arrays.asList(
                        new FieldAccessPostAggregator("idx", "idx"),
                        new ConstantPostAggregator("const", 1, null)
                    )
                ),
                new ArithmeticPostAggregator(
                    "subtractStuff",
                    "-",
                    Arrays.asList(
                        new FieldAccessPostAggregator("rev", "rev2"),
                        new ConstantPostAggregator("const", 1, null)
                    )
                )
            )
        ),
        new ArithmeticPostAggregator(
            "addStuff",
            "+",
            Arrays.<PostAggregator>asList(
                new FieldAccessPostAggregator("divideStuff", "divideStuff"),
                new FieldAccessPostAggregator("count", "count")
            )
        )
    );

    boolean exceptionOccured = false;

    try {
      Queries.verifyAggregations(aggFactories, postAggs);
    }
    catch (IllegalArgumentException e) {
      exceptionOccured = true;
    }

    Assert.assertTrue(exceptionOccured);
  }
}
