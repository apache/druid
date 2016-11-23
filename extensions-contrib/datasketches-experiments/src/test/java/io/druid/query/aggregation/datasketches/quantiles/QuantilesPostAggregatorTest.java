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

package io.druid.query.aggregation.datasketches.quantiles;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.query.aggregation.PostAggregator;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class QuantilesPostAggregatorTest
{
  private final ObjectMapper mapper;

  public QuantilesPostAggregatorTest()
  {
    mapper = QuantilesSketchAggregatorFactoryTest.buildObjectMapper();
  }

  @Test
  public void testSerde() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"datasketchesQuantiles\",\n"
                     + "  \"name\": \"testName\",\n"
                     + "  \"fractions\": [0.1,0.5],\n"
                     + "  \"fieldName\": \"testFieldName\""
                     + "}\n";

    assertPostAggregatorSerde(
        mapper,
        jsonStr,
        new QuantilesPostAggregator(
            "testName",
            new double[]{0.1, 0.5},
            "testFieldName"
        )
    );
  }

  public static void assertPostAggregatorSerde(ObjectMapper mapper, String postAggJson, PostAggregator expected)
      throws Exception
  {
    Assert.assertEquals(
        expected,
        mapper.readValue(
            mapper.writeValueAsString(
                mapper.readValue(postAggJson, PostAggregator.class)
            ),
            PostAggregator.class
        )
    );
  }
}
