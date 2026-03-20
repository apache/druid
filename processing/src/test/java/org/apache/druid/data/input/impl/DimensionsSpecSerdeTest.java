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

import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.guice.DruidSecondaryModule;
import org.apache.druid.guice.GuiceAnnotationIntrospector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 */
public class DimensionsSpecSerdeTest
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    AnnotationIntrospector introspector = new GuiceAnnotationIntrospector();
    DruidSecondaryModule.setupAnnotationIntrospector(OBJECT_MAPPER, introspector);
    OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Test
  public void testDimensionsSpecSerde() throws Exception
  {
    DimensionsSpec expected = DimensionsSpec
        .builder()
        .setDimensions(
            Arrays.asList(
                new StringDimensionSchema("AAA"),
                new StringDimensionSchema("BBB"),
                new FloatDimensionSchema("C++"),
                new NewSpatialDimensionSchema("DDT", null),
                new LongDimensionSchema("EEE"),
                new NewSpatialDimensionSchema("DDT2", Arrays.asList("A", "B")),
                new NewSpatialDimensionSchema("IMPR", Arrays.asList("S", "P", "Q", "R"))
            )
        )
        .setDimensionExclusions(Arrays.asList("FOO", "HAR"))
        .build();

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

  @Test
  public void testToBuilder()
  {
    DimensionsSpec original = DimensionsSpec
        .builder()
        .setDimensions(
            Arrays.asList(
                new StringDimensionSchema("dim1"),
                new LongDimensionSchema("dim2"),
                new FloatDimensionSchema("dim3")
            )
        )
        .setDimensionExclusions(Arrays.asList("ex1", "ex2"))
        .setIncludeAllDimensions(true)
        .useSchemaDiscovery(false)
        .setForceSegmentSortByTime(true)
        .build();

    DimensionsSpec rebuilt = original.toBuilder().build();

    Assert.assertEquals(original, rebuilt);
    Assert.assertEquals(original.getDimensions(), rebuilt.getDimensions());
    Assert.assertEquals(original.getDimensionExclusions(), rebuilt.getDimensionExclusions());
    Assert.assertEquals(original.isIncludeAllDimensions(), rebuilt.isIncludeAllDimensions());
    Assert.assertEquals(original.useSchemaDiscovery(), rebuilt.useSchemaDiscovery());
    Assert.assertEquals(original.isForceSegmentSortByTimeConfigured(), rebuilt.isForceSegmentSortByTimeConfigured());
  }

  @Test
  public void testToBuilderWithModifications()
  {
    DimensionsSpec original = DimensionsSpec
        .builder()
        .setDimensions(
            Arrays.asList(
                new StringDimensionSchema("dim1"),
                new LongDimensionSchema("dim2")
            )
        )
        .setDimensionExclusions(List.of("ex1"))
        .setIncludeAllDimensions(false)
        .useSchemaDiscovery(false)
        .setForceSegmentSortByTime(true)
        .build();

    DimensionsSpec modified = original.toBuilder()
        .setDimensions(
            Arrays.asList(
                new StringDimensionSchema("dim1"),
                new LongDimensionSchema("dim2"),
                new FloatDimensionSchema("dim3")
            )
        )
        .setDimensionExclusions(Arrays.asList("ex1", "ex2"))
        .setForceSegmentSortByTime(false)
        .build();

    Assert.assertNotEquals(original, modified);
    Assert.assertNotEquals(original.getDimensions().size(), modified.getDimensions().size());
    Assert.assertEquals(3, modified.getDimensions().size());
    Assert.assertNotEquals(original.getDimensionExclusions().size(), modified.getDimensionExclusions().size());
    Assert.assertEquals(2, modified.getDimensionExclusions().size());
    Assert.assertNotEquals(original.isForceSegmentSortByTimeConfigured(), modified.isForceSegmentSortByTimeConfigured());
  }

  @Test
  public void testToBuilderPreservesAllFields()
  {
    DimensionsSpec original = DimensionsSpec
        .builder()
        .setDimensions(
            Arrays.asList(
                new StringDimensionSchema("dim1"),
                new NewSpatialDimensionSchema("spatial1", Arrays.asList("x", "y"))
            )
        )
        .setDimensionExclusions(Arrays.asList("ex1", "ex2", "ex3"))
        .setIncludeAllDimensions(true)
        .useSchemaDiscovery(true)
        .setForceSegmentSortByTime(false)
        .build();

    DimensionsSpec rebuilt = original.toBuilder().build();

    Assert.assertEquals(original.getDimensions(), rebuilt.getDimensions());
    Assert.assertEquals(original.getDimensionExclusions(), rebuilt.getDimensionExclusions());
    Assert.assertEquals(original.isIncludeAllDimensions(), rebuilt.isIncludeAllDimensions());
    Assert.assertEquals(original.useSchemaDiscovery(), rebuilt.useSchemaDiscovery());
    Assert.assertEquals(original.isForceSegmentSortByTimeConfigured(), rebuilt.isForceSegmentSortByTimeConfigured());
    Assert.assertEquals(original.getSpatialDimensions(), rebuilt.getSpatialDimensions());
  }
}
