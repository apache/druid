package com.metamx.druid.query;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.post.PostAggregator;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

/**
 */
public class Queries
{
  public static void verifyAggregations(
      List<AggregatorFactory> aggFactories,
      List<PostAggregator> postAggs
  )
  {
    Preconditions.checkNotNull(aggFactories, "aggregations cannot be null");
    Preconditions.checkArgument(aggFactories.size() > 0, "Must have at least one AggregatorFactory");

    if (postAggs != null && !postAggs.isEmpty()) {
      Set<String> combinedAggNames = Sets.newHashSet(
          Lists.transform(
              aggFactories,
              new Function<AggregatorFactory, String>()
              {
                @Override
                public String apply(@Nullable AggregatorFactory input)
                {
                  return input.getName();
                }
              }
          )
      );

      for (PostAggregator postAgg : postAggs) {
        Preconditions.checkArgument(
            postAgg.verifyFields(combinedAggNames),
            String.format("Missing field[%s]", postAgg.getName())
        );
        combinedAggNames.add(postAgg.getName());
      }
    }
  }
}
