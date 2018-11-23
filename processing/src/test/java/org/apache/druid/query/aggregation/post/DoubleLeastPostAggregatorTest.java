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

package org.apache.druid.query.aggregation.post;

import com.google.common.collect.Lists;
import org.apache.druid.query.aggregation.CountAggregator;
import org.apache.druid.query.aggregation.PostAggregator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class DoubleLeastPostAggregatorTest
{
  @Test
  public void testCompute()
  {
    final String aggName = "rows";
    DoubleLeastPostAggregator leastPostAggregator;
    CountAggregator agg = new CountAggregator();
    agg.aggregate();
    agg.aggregate();
    agg.aggregate();
    Map<String, Object> metricValues = new HashMap<String, Object>();
    metricValues.put(aggName, agg.get());

    List<PostAggregator> postAggregatorList =
        Lists.newArrayList(
            new ConstantPostAggregator(
                "roku", 6D
            ),
            new FieldAccessPostAggregator(
                "rows", aggName
            )
        );

    leastPostAggregator = new DoubleLeastPostAggregator("least", postAggregatorList);
    Assert.assertEquals(3.0, leastPostAggregator.compute(metricValues));
  }

  @Test
  public void testComparator()
  {
    final String aggName = "rows";
    DoubleLeastPostAggregator leastPostAggregator;
    CountAggregator agg = new CountAggregator();
    Map<String, Object> metricValues = new HashMap<String, Object>();
    metricValues.put(aggName, agg.get());

    List<PostAggregator> postAggregatorList =
        Lists.newArrayList(
            new ConstantPostAggregator(
                "roku", 2D
            ),
            new FieldAccessPostAggregator(
                "rows", aggName
            )
        );

    leastPostAggregator = new DoubleLeastPostAggregator("least", postAggregatorList);
    Comparator comp = leastPostAggregator.getComparator();
    Object before = leastPostAggregator.compute(metricValues);
    agg.aggregate();
    agg.aggregate();
    agg.aggregate();
    metricValues.put(aggName, agg.get());
    Object after = leastPostAggregator.compute(metricValues);

    Assert.assertEquals(-1, comp.compare(before, after));
    Assert.assertEquals(0, comp.compare(before, before));
    Assert.assertEquals(0, comp.compare(after, after));
    Assert.assertEquals(1, comp.compare(after, before));
  }
}
