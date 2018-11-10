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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.junit.Assert;
import org.junit.Test;

public class PvaluefromZscorePostAggregatorTest
{
  PvaluefromZscorePostAggregator pvaluefromZscorePostAggregator;
  ConstantPostAggregator zscore;

  @Test
  public void testPvaluefromZscorePostAggregator()
  {
    zscore = new ConstantPostAggregator("zscore", -1783.8762354220219);

    pvaluefromZscorePostAggregator = new PvaluefromZscorePostAggregator("pvalue", zscore);

    double pvalue = ((Number) pvaluefromZscorePostAggregator.compute(ImmutableMap.of(
        "zscore",
        -1783.8762354220219
    ))).doubleValue();

    /* Assert P-value is positive and very small */
    Assert.assertTrue(pvalue >= 0 && pvalue < 0.00001);
  }

  @Test
  public void testSerde() throws Exception
  {
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    PvaluefromZscorePostAggregator postAggregator1 = mapper.readValue(
        mapper.writeValueAsString(pvaluefromZscorePostAggregator),
        PvaluefromZscorePostAggregator.class
    );

    Assert.assertEquals(pvaluefromZscorePostAggregator, postAggregator1);
  }

}
