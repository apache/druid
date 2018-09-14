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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 */
public class DimensionsSpecSerdeTest
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Test
  public void testDimensionsSpecSerde() throws Exception
  {
    DimensionsSpec expected = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("AAA"),
            new StringDimensionSchema("BBB"),
            new FloatDimensionSchema("C++"),
            new NewSpatialDimensionSchema("DDT", null),
            new LongDimensionSchema("EEE"),
            new NewSpatialDimensionSchema("DDT2", Arrays.asList("A", "B")),
            new NewSpatialDimensionSchema("IMPR", Arrays.asList("S", "P", "Q", "R"))
        ),
        Arrays.asList("FOO", "HAR"),
        null
    );

    String jsonStr = "{\"dimensions\":"
                     + "[\"AAA\", \"BBB\","
                     + "{\"name\":\"C++\", \"type\":\"float\"},"
                     + "{\"name\":\"DDT\", \"type\":\"spatial\"},"
                     + "{\"name\":\"EEE\", \"type\":\"long\"},"
                     + "{\"name\":\"DDT2\", \"type\": \"spatial\", \"dims\":[\"A\", \"B\"]}],"
                     + "\"dimensionExclusions\": [\"FOO\", \"HAR\"],"
                     + "\"spatialDimensions\": [{\"dimName\":\"IMPR\", \"dims\":[\"S\",\"P\",\"Q\",\"R\"]}]"
                     + "}";

    DimensionsSpec actual = OBJECT_MAPPER.readValue(
        OBJECT_MAPPER.writeValueAsString(
            OBJECT_MAPPER.readValue(jsonStr, DimensionsSpec.class)
        ),
        DimensionsSpec.class
    );

    List<SpatialDimensionSchema> expectedSpatials = Arrays.asList(
        new SpatialDimensionSchema("DDT", null),
        new SpatialDimensionSchema("DDT2", Arrays.asList("A", "B")),
        new SpatialDimensionSchema("IMPR", Arrays.asList("S", "P", "Q", "R"))
    );

    Assert.assertEquals(expected, actual);
    Assert.assertEquals(expectedSpatials, actual.getSpatialDimensions());
  }
}
