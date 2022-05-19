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

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;

public class QuantilesTest
{
  @Test
  public void testSerialization() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    float[] probabilities = new float[]{0.25f, 0.5f, 0.75f};
    float[] quantiles = new float[]{0.25f, 0.5f, 0.75f};
    float min = 0f;
    float max = 4f;

    String theString = mapper.writeValueAsString(
        new Quantiles(probabilities, quantiles, min, max)
    );

    Object theObject = mapper.readValue(theString, Object.class);
    Assert.assertThat(theObject, CoreMatchers.instanceOf(LinkedHashMap.class));

    LinkedHashMap theMap = (LinkedHashMap) theObject;

    ArrayList theProbabilities = (ArrayList<Float>) theMap.get("probabilities");

    Assert.assertEquals(probabilities.length, theProbabilities.size());
    for (int i = 0; i < theProbabilities.size(); ++i) {
      Assert.assertEquals(probabilities[i], ((Number) theProbabilities.get(i)).floatValue(), 0.0001f);
    }

    ArrayList theQuantiles = (ArrayList<Float>) theMap.get("quantiles");

    Assert.assertEquals(quantiles.length, theQuantiles.size());
    for (int i = 0; i < theQuantiles.size(); ++i) {
      Assert.assertEquals(quantiles[i], ((Number) theQuantiles.get(i)).floatValue(), 0.0001f);
    }

    Assert.assertEquals(
        "serialized min. matches expected min.",
        min,
        ((Number) theMap.get("min")).floatValue(),
        0.0001f
    );
    Assert.assertEquals(
        "serialized max. matches expected max.",
        max,
        ((Number) theMap.get("max")).floatValue(),
        0.0001f
    );


  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(Quantiles.class)
                  .withNonnullFields("probabilities", "quantiles", "min", "max")
                  .usingGetClass()
                  .verify();
  }
}
