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
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class QuantilesPostAggregatorTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSerde() throws Exception
  {
    QuantilesPostAggregator there =
        new QuantilesPostAggregator("max", "test_field", new float[]{0.2f, 0.7f});

    DefaultObjectMapper mapper = new DefaultObjectMapper();
    QuantilesPostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        QuantilesPostAggregator.class
    );

    Assert.assertEquals(there, andBackAgain);
    Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
    Assert.assertEquals(there.getDependentFields(), andBackAgain.getDependentFields());
  }

  @Test
  public void testComparator()
  {
    expectedException.expect(UnsupportedOperationException.class);
    QuantilesPostAggregator quantiles = new QuantilesPostAggregator("quantiles", "someAgg", new float[]{0.3f, 0.9f});
    quantiles.getComparator();
  }

  @Test
  public void testToString()
  {
    QuantilesPostAggregator postAgg =
        new QuantilesPostAggregator("post", "test_field", new float[]{0.2f, 0.7f});

    Assert.assertEquals(
        "QuantilesPostAggregator{name='post', fieldName='test_field', probabilities=[0.2, 0.7]}",
        postAgg.toString()
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(QuantilesPostAggregator.class)
                  .withNonnullFields("name", "fieldName")
                  .usingGetClass()
                  .verify();
  }
}
