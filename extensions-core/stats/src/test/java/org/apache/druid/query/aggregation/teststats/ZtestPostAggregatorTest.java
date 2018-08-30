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

package org.apache.druid.query.aggregation.teststats;

import com.google.common.collect.Lists;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZtestPostAggregatorTest
{
  ZtestPostAggregator ztestPostAggregator;

  @Test
  public void testZtestPostAggregator()
  {
    ConstantPostAggregator successCount1, sample1Size, successCount2, sample2Size;

    successCount1 = new ConstantPostAggregator("successCountPopulation1", 39244);
    sample1Size = new ConstantPostAggregator("sampleSizePopulation1", 394298);
    successCount2 = new ConstantPostAggregator("successCountPopulation2", 8991275);
    sample2Size = new ConstantPostAggregator("sampleSizePopulation2", 9385573);

    List<PostAggregator> postAggregatorList;
    postAggregatorList = Lists.newArrayList(
        successCount1,
        sample1Size,
        successCount2,
        sample2Size
    );

    Map<String, Object> metricValues = new HashMap<>();
    for (PostAggregator pa : postAggregatorList) {
      metricValues.put(pa.getName(), ((ConstantPostAggregator) pa).getConstantValue());
    }

    ztestPostAggregator = new ZtestPostAggregator(
        "zscore",
        successCount1,
        sample1Size,
        successCount2,
        sample2Size
    );

    double zscore = ((Number) ztestPostAggregator.compute(metricValues)).doubleValue();

    Assert.assertEquals(-1783.8762354220219,
                        zscore, 0.0001
    );
  }

  @Test
  public void testSerde() throws Exception
  {
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    ZtestPostAggregator postAggregator1 =
        mapper.readValue(
            mapper.writeValueAsString(ztestPostAggregator),
            ZtestPostAggregator.class
        );

    Assert.assertEquals(ztestPostAggregator, postAggregator1);
  }
}
