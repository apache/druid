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

package org.apache.druid.query.aggregation.variance;

import org.apache.druid.com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class StandardDeviationPostAggregatorTest extends InitializedNullHandlingTest
{
  private static final String NAME = "NAME";
  private static final String FIELD_NAME = "FIELD_NAME";
  private static final String POPULATION = "population";
  private static final double VARIANCE = 12.56;

  private Map<String, Object> combinedAggregators;
  @Mock
  private VarianceAggregatorCollector collector;

  private StandardDeviationPostAggregator target;

  @Before
  public void setUp()
  {
    Mockito.doReturn(VARIANCE).when(collector).getVariance(true);
    combinedAggregators = ImmutableMap.of(FIELD_NAME, collector);
    target = new StandardDeviationPostAggregator(NAME, FIELD_NAME, POPULATION);
  }

  @Test
  public void testSerde() throws Exception
  {
    StandardDeviationPostAggregator there =
        new StandardDeviationPostAggregator("post", "test_field", "population");

    DefaultObjectMapper mapper = new DefaultObjectMapper();
    StandardDeviationPostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        StandardDeviationPostAggregator.class
    );

    Assert.assertEquals(there, andBackAgain);
    Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
    Assert.assertEquals(there.getDependentFields(), andBackAgain.getDependentFields());
  }

  @Test
  public void testToString()
  {
    PostAggregator postAgg =
        new StandardDeviationPostAggregator("post", "test_field", "population");

    Assert.assertEquals(
        "StandardDeviationPostAggregator{name='post', fieldName='test_field', estimator='population', isVariancePop=true}",
        postAgg.toString()
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(StandardDeviationPostAggregator.class)
                  .withNonnullFields("name", "fieldName")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testComputeForNullVarianceShouldReturnDefaultDoubleValue()
  {
    Mockito.when(collector.getVariance(true)).thenReturn(null);
    Assert.assertEquals(NullHandling.defaultDoubleValue(), target.compute(combinedAggregators));
  }

  @Test
  public void testComputeForVarianceShouldReturnSqrtOfVariance()
  {
    Assert.assertEquals(Math.sqrt(VARIANCE), target.compute(combinedAggregators), 1e-15);
  }
}
