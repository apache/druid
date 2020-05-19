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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

public class PvaluefromZscorePostAggregatorTest
{
  PvaluefromZscorePostAggregator pvaluefromZscorePostAggregator;
  FieldAccessPostAggregator zscore;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup()
  {
    zscore = new FieldAccessPostAggregator("f1", "zscore");
    pvaluefromZscorePostAggregator = new PvaluefromZscorePostAggregator("pvalue", zscore);
  }

  @Test
  public void testPvaluefromZscorePostAggregator()
  {
    Map<String, Object> row = ImmutableMap.of("zscore", -1783.8762354220219);
    double pvalue = ((Number) pvaluefromZscorePostAggregator.compute(row)).doubleValue();

    /* Assert P-value is positive and very small */
    Assert.assertTrue(pvalue >= 0 && pvalue < 0.00001);
  }

  @Test
  public void testPvaluefromNullZscorePostAggregator()
  {
    Map<String, Object> row = new HashMap<>();
    row.put("zscore", null);
    Object result = pvaluefromZscorePostAggregator.compute(row);
    Assert.assertNull(result);
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
    Assert.assertArrayEquals(pvaluefromZscorePostAggregator.getCacheKey(), postAggregator1.getCacheKey());
    Assert.assertEquals(pvaluefromZscorePostAggregator.getDependentFields(), postAggregator1.getDependentFields());
  }

  @Test
  public void testToString()
  {
    Assert.assertEquals(
        "PvaluefromZscorePostAggregator{name='pvalue', zScore=FieldAccessPostAggregator{name='f1', fieldName='zscore'}}",
        pvaluefromZscorePostAggregator.toString()
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(PvaluefromZscorePostAggregator.class)
                  .withNonnullFields("name", "zScore")
                  .usingGetClass()
                  .verify();
  }
}
