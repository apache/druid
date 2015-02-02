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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;

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
                public String apply(AggregatorFactory input)
                {
                  return input.getName();
                }
              }
          )
      );

      for (PostAggregator postAgg : postAggs) {
        Set<String> dependencies = postAgg.getDependentFields();
        Set<String> missing = Sets.difference(dependencies, combinedAggNames);

        Preconditions.checkArgument(
            missing.isEmpty(),
            "Missing fields [%s] for postAggregator [%s]", missing, postAgg.getName()
        );
        combinedAggNames.add(postAgg.getName());
      }
    }
  }
}
