/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.teststats;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by chunchen on 4/23/17.
 */
public class TestStatsTest {

  @Test
  public void testCompute() {
    ZtestPostAggregator ztestPostAggregator;
    PvaluefromZscorePostAggregator pvaluePostAggregator;
    ConstantPostAggregator constPostAgg1, constPostAgg2, constPostAgg3, constPostAgg4;

    constPostAgg1 = new ConstantPostAggregator("successCountPopulation1", 39244);
    constPostAgg2 = new ConstantPostAggregator("sampleSizePopulation1", 394298);
    constPostAgg3 = new ConstantPostAggregator("successCountPopulation2", 8991275);
    constPostAgg4 = new ConstantPostAggregator("sampleSizePopulation2", 9385573);

    List<PostAggregator> postAggregatorList =
        Lists.newArrayList(
            (PostAggregator) constPostAgg1,
            constPostAgg2,
            constPostAgg3,
            constPostAgg4
        );

    Map<String, Object> metricValues = new HashMap<String, Object>();
    for (PostAggregator pa : postAggregatorList) {
      metricValues.put(pa.getName(), ((ConstantPostAggregator) pa).getConstantValue());
    }

    ztestPostAggregator = new ZtestPostAggregator("zscore", postAggregatorList);

    double zscore = ((Number) ztestPostAggregator.compute(metricValues)).doubleValue();

    pvaluePostAggregator = new PvaluefromZscorePostAggregator("pvalue", ztestPostAggregator);

    System.out.print("zscore = " + zscore + "\n");
    System.out.print("pvalue = " +
        pvaluePostAggregator.compute(ImmutableMap.<String, Object>of("zscore", -1783.8762354220219)));

    Assert.assertEquals(-1783.8762354220219,
        zscore, 0.0001);
    Assert.assertNotEquals(0.0,
        ztestPostAggregator.compute(metricValues));
  }
}
