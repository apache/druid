/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
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

package io.druid.query.aggregation;

import com.google.common.collect.Lists;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class AggregatorUtilTest
{

  @Test
  public void testPruneDependentPostAgg()
  {
    PostAggregator agg1 = new ArithmeticPostAggregator(
        "abc", "+", Lists.<PostAggregator>newArrayList(
        new ConstantPostAggregator("1", 1L, 1L), new ConstantPostAggregator("2", 2L, 2L)
    )
    );
    PostAggregator dependency1 = new ArithmeticPostAggregator(
        "dep1", "+", Lists.<PostAggregator>newArrayList(
        new ConstantPostAggregator("1", 1L, 1L), new ConstantPostAggregator("4", 4L, 4L)
    )
    );
    PostAggregator agg2 = new FieldAccessPostAggregator("def", "def");
    PostAggregator dependency2 = new FieldAccessPostAggregator("dep2", "dep2");
    PostAggregator aggregator = new ArithmeticPostAggregator(
        "finalAgg",
        "+",
        Lists.<PostAggregator>newArrayList(
            new FieldAccessPostAggregator("dep1", "dep1"),
            new FieldAccessPostAggregator("dep2", "dep2")
        )
    );
    List<PostAggregator> prunedAgg = AggregatorUtil.pruneDependentPostAgg(
        Lists.newArrayList(
            agg1,
            dependency1,
            agg2,
            dependency2,
            aggregator
        ), aggregator.getName()
    );
    Assert.assertEquals(Lists.newArrayList(dependency1, dependency2, aggregator), prunedAgg);
  }

}
