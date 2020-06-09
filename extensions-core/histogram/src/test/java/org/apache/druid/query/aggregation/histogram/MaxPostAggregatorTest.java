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
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class MaxPostAggregatorTest extends InitializedNullHandlingTest
{
  @Test
  public void testSerde() throws Exception
  {
    MaxPostAggregator there =
        new MaxPostAggregator("max", "test_field");

    DefaultObjectMapper mapper = new DefaultObjectMapper();
    MaxPostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        MaxPostAggregator.class
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

    MaxPostAggregator max = new MaxPostAggregator("max", aggName);
    Comparator comp = max.getComparator();
    ApproximateHistogram histo1 = new ApproximateHistogram();
    metricValues.put(aggName, histo1);

    Object before = max.compute(metricValues);

    ApproximateHistogram histo2 = new ApproximateHistogram();
    histo2.offer(1.0f);
    metricValues.put(aggName, histo2);
    Object after = max.compute(metricValues);

    Assert.assertEquals(-1, comp.compare(before, after));
    Assert.assertEquals(0, comp.compare(before, before));
    Assert.assertEquals(0, comp.compare(after, after));
    Assert.assertEquals(1, comp.compare(after, before));
  }

  @Test
  public void testToString()
  {
    PostAggregator postAgg =
        new MaxPostAggregator("max", "test_field");

    Assert.assertEquals(
        "MaxPostAggregator{fieldName='test_field'}",
        postAgg.toString()
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(MaxPostAggregator.class)
                  .withNonnullFields("name", "fieldName")
                  .usingGetClass()
                  .verify();
  }
}
