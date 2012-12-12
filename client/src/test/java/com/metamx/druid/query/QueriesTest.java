package com.metamx.druid.query;

import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.CountAggregatorFactory;
import com.metamx.druid.aggregation.DoubleSumAggregatorFactory;
import com.metamx.druid.aggregation.post.ArithmeticPostAggregator;
import com.metamx.druid.aggregation.post.ConstantPostAggregator;
import com.metamx.druid.aggregation.post.FieldAccessPostAggregator;
import com.metamx.druid.aggregation.post.PostAggregator;
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
    catch (Exception e) {
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
    catch (Exception e) {
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
                        new ConstantPostAggregator("const", 1)
                    )
                ),
                new ArithmeticPostAggregator(
                    "subtractStuff",
                    "-",
                    Arrays.asList(
                        new FieldAccessPostAggregator("rev", "rev"),
                        new ConstantPostAggregator("const", 1)
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
    catch (Exception e) {
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
                        new ConstantPostAggregator("const", 1)
                    )
                ),
                new ArithmeticPostAggregator(
                    "subtractStuff",
                    "-",
                    Arrays.asList(
                        new FieldAccessPostAggregator("rev", "rev2"),
                        new ConstantPostAggregator("const", 1)
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
    catch (Exception e) {
      exceptionOccured = true;
    }

    Assert.assertTrue(exceptionOccured);
  }
}
