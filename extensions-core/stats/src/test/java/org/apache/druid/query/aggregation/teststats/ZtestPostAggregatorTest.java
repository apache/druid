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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ZtestPostAggregatorTest
{
  FieldAccessPostAggregator successCount1;
  FieldAccessPostAggregator sample1Size;
  FieldAccessPostAggregator successCount2;
  FieldAccessPostAggregator sample2Size;
  ZtestPostAggregator ztestPostAggregator;

  @Before
  public void setup()
  {
    successCount1 = new FieldAccessPostAggregator("sc1", "successCountPopulation1");
    sample1Size = new FieldAccessPostAggregator("ss1", "sampleSizePopulation1");
    successCount2 = new FieldAccessPostAggregator("sc2", "successCountPopulation2");
    sample2Size = new FieldAccessPostAggregator("ss2", "sampleSizePopulation2");

    ztestPostAggregator = new ZtestPostAggregator(
        "zscore",
        successCount1,
        sample1Size,
        successCount2,
        sample2Size
    );
  }

  @Test
  public void testZtestPostAggregator()
  {
    Map<String, Object> metricValues = new HashMap<>();

    Object result = ztestPostAggregator.compute(metricValues);
    Assert.assertNull(result);

    metricValues.put("successCountPopulation1", 39244);
    result = ztestPostAggregator.compute(metricValues);
    Assert.assertNull(result);

    metricValues.put("sampleSizePopulation1", 394298);
    result = ztestPostAggregator.compute(metricValues);
    Assert.assertNull(result);

    metricValues.put("successCountPopulation2", 8991275);
    result = ztestPostAggregator.compute(metricValues);
    metricValues.put("sampleSizePopulation2", 9385573);
    Assert.assertNull(result);

    double zscore = ((Number) ztestPostAggregator.compute(metricValues)).doubleValue();
    Assert.assertEquals(-1783.8762354220219, zscore, 0.0001);
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
    Assert.assertArrayEquals(ztestPostAggregator.getCacheKey(), postAggregator1.getCacheKey());
    Assert.assertEquals(ztestPostAggregator.getDependentFields(), postAggregator1.getDependentFields());
  }

  @Test
  public void testToString()
  {
    Assert.assertEquals(
        "ZtestPostAggregator{name='zscore', successCount1='FieldAccessPostAggregator{name='sc1', fieldName='successCountPopulation1'}', sample1Size='FieldAccessPostAggregator{name='ss1', fieldName='sampleSizePopulation1'}', successCount2='FieldAccessPostAggregator{name='sc2', fieldName='successCountPopulation2'}', sample2size='FieldAccessPostAggregator{name='ss2', fieldName='sampleSizePopulation2'}}",
        ztestPostAggregator.toString()
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(ZtestPostAggregator.class)
                  .withNonnullFields("name", "successCount1", "sample1Size", "successCount2", "sample2Size")
                  .usingGetClass()
                  .verify();
  }
}
