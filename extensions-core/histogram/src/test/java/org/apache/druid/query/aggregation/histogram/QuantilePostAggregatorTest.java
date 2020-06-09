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

package org.apache.druid.query.aggregation.histogram;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class QuantilePostAggregatorTest extends InitializedNullHandlingTest
{
  @Test
  public void testSerde() throws Exception
  {
    QuantilePostAggregator there =
        new QuantilePostAggregator("max", "test_field", 0.5f);

    DefaultObjectMapper mapper = new DefaultObjectMapper();
    QuantilePostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        QuantilePostAggregator.class
    );

    Assert.assertEquals(there, andBackAgain);
    Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
    Assert.assertEquals(there.getDependentFields(), andBackAgain.getDependentFields());
  }

  @Test
  public void testComparator()
  {
    final String aggName = "doubleWithNulls";
    Map<String, Object> metricValues = new HashMap<>();

    QuantilePostAggregator quantile = new QuantilePostAggregator("quantile", aggName, 0.9f);
    Comparator comp = quantile.getComparator();
    ApproximateHistogram histo1 = new ApproximateHistogram();
    histo1.offer(10.0f);
    metricValues.put(aggName, histo1);

    Object before = quantile.compute(metricValues);

    ApproximateHistogram histo2 = new ApproximateHistogram();
    histo2.offer(100.0f);
    metricValues.put(aggName, histo2);

    Object after = quantile.compute(metricValues);

    Assert.assertEquals(-1, comp.compare(before, after));
    Assert.assertEquals(0, comp.compare(before, before));
    Assert.assertEquals(0, comp.compare(after, after));
    Assert.assertEquals(1, comp.compare(after, before));
  }

  @Test
  public void testToString()
  {
    QuantilePostAggregator postAgg =
        new QuantilePostAggregator("quantile", "testField", 0.9f);

    Assert.assertEquals(
        "QuantilePostAggregator{name='quantile', fieldName='testField', probability=0.9}",
        postAgg.toString()
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(QuantilePostAggregator.class)
                  .withNonnullFields("name", "fieldName")
                  .usingGetClass()
                  .verify();
  }
}
