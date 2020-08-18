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

package org.apache.druid.query.topn;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.DoubleGreatestPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class NumericTopNMetricSpecTest
{

  @Test
  public void testGetComparator()
  {
    final AggregatorFactory agg = new DoubleSumAggregatorFactory("agg", "agg");
    List<AggregatorFactory> aggregatorSpecs = Collections.singletonList(agg);

    final PostAggregator postAgg = new DoubleGreatestPostAggregator(
        "postAgg",
        Collections.singletonList(new FieldAccessPostAggregator("agg", "agg"))
    );
    List<PostAggregator> postAggregatorSpecs = Collections.singletonList(postAgg);

    NumericTopNMetricSpec aggSpec = new NumericTopNMetricSpec("agg");
    Comparator aggComp = aggSpec.getComparator(aggregatorSpecs, postAggregatorSpecs);
    Assert.assertNotNull(aggComp);
    Assert.assertTrue(aggComp == agg.getComparator());

    NumericTopNMetricSpec postAggSpec = new NumericTopNMetricSpec("postAgg");
    Comparator postAggComp = postAggSpec.getComparator(aggregatorSpecs, postAggregatorSpecs);
    Assert.assertNotNull(postAggComp);
    Assert.assertTrue(postAggComp == postAgg.getComparator());
  }

}
