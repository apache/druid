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

package org.apache.druid.server.http;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.junit.Assert;
import org.junit.Test;

public class CoordinatorDynamicConfigTest
{
  private final ObjectMapper mapper = TestHelper.makeJsonMapper();

  @Test
  public void testEqualsAndHashCodeSanity()
  {
    CoordinatorDynamicConfig config1 = CoordinatorDynamicConfig.builder().build();
    CoordinatorDynamicConfig config2 = CoordinatorDynamicConfig.builder().build();
    Assert.assertEquals(config1, config2);
    Assert.assertEquals(config1.hashCode(), config2.hashCode());
  }

  @Test
  public void testSerdeFailForInvalidPercentOfSegmentsToConsiderPerMove() throws Exception
  {
    try {
      String jsonStr = "{\n"
                       + "  \"percentOfSegmentsToConsiderPerMove\": 0\n"
                       + "}\n";

      mapper.readValue(
          mapper.writeValueAsString(
              mapper.readValue(
                  jsonStr,
                  CoordinatorDynamicConfig.class
              )
          ),
          CoordinatorDynamicConfig.class
      );

      Assert.fail("deserialization should fail.");
    }
    catch (JsonMappingException e) {
      Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
    }

    try {
      String jsonStr = "{\n"
                       + "  \"percentOfSegmentsToConsiderPerMove\": -100\n"
                       + "}\n";

      mapper.readValue(
          mapper.writeValueAsString(
              mapper.readValue(
                  jsonStr,
                  CoordinatorDynamicConfig.class
              )
          ),
          CoordinatorDynamicConfig.class
      );

      Assert.fail("deserialization should fail.");
    }
    catch (JsonMappingException e) {
      Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
    }

    try {
      String jsonStr = "{\n"
                       + "  \"percentOfSegmentsToConsiderPerMove\": 105\n"
                       + "}\n";

      mapper.readValue(
          mapper.writeValueAsString(
              mapper.readValue(
                  jsonStr,
                  CoordinatorDynamicConfig.class
              )
          ),
          CoordinatorDynamicConfig.class
      );

      Assert.fail("deserialization should fail.");
    }
    catch (JsonMappingException e) {
      Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
    }
  }

  @Test
  public void testSerdeFailForInvalidDecommissioningMaxPercentOfSegmentsToConsiderToMove() throws Exception
  {
    try {
      String jsonStr = "{\n"
                       + "  \"decommissioningMaxPercentOfMaxSegmentsToMove\": -100\n"
                       + "}\n";

      mapper.readValue(
          mapper.writeValueAsString(
              mapper.readValue(
                  jsonStr,
                  CoordinatorDynamicConfig.class
              )
          ),
          CoordinatorDynamicConfig.class
      );

      Assert.fail("deserialization should fail.");
    }
    catch (JsonMappingException e) {
      Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
    }

    try {
      String jsonStr = "{\n"
                       + "  \"decommissioningMaxPercentOfMaxSegmentsToMove\": 105\n"
                       + "}\n";

      mapper.readValue(
          mapper.writeValueAsString(
              mapper.readValue(
                  jsonStr,
                  CoordinatorDynamicConfig.class
              )
          ),
          CoordinatorDynamicConfig.class
      );

      Assert.fail("deserialization should fail.");
    }
    catch (JsonMappingException e) {
      Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
    }
  }

  @Test
  public void testSerdeFailForInvalidMaxNonPrimaryReplicantsToLoad() throws Exception
  {
    try {
      String jsonStr = "{\n"
                       + "  \"maxNonPrimaryReplicantsToLoad\": -1\n"
                       + "}\n";

      mapper.readValue(
          mapper.writeValueAsString(
              mapper.readValue(
                  jsonStr,
                  CoordinatorDynamicConfig.class
              )
          ),
          CoordinatorDynamicConfig.class
      );

      Assert.fail("deserialization should fail.");
    }
    catch (JsonMappingException e) {
      Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
    }
  }

  @Test
  public void testSerdeFailForInvalidMaxPrimaryReplicantsToLoad() throws Exception
  {
    try {
      String jsonStr = "{\n"
                       + "  \"maxSegmentsToLoadPerCoordinationCycle\": -1\n"
                       + "}\n";

      mapper.readValue(
          mapper.writeValueAsString(
              mapper.readValue(
                  jsonStr,
                  CoordinatorDynamicConfig.class
              )
          ),
          CoordinatorDynamicConfig.class
      );

      Assert.fail("deserialization should fail.");
    }
    catch (JsonMappingException e) {
      Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
    }
  }

  @Test
  public void testDynamicSerdeOfSpecificDataSourcesToKillUnusedSegmentsIn() throws Exception
  {
    String jsonStr = "{\n" +
                     " \"killDataSourceWhitelist\": \"test1, test2\"\n" +
                     "}\n";
    CoordinatorDynamicConfig actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                CoordinatorDynamicConfig.class
            )
        ),
        CoordinatorDynamicConfig.class
    );
    Assert.assertEquals(ImmutableSet.of("test1", "test2"), actual.getSpecificDataSourcesToKillUnusedSegmentsIn());

    jsonStr = "{\n" +
              " \"killDataSourceWhitelist\": [\"test1\", \"test2\"]\n" +
              "}\n";
    actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                CoordinatorDynamicConfig.class
            )
        ),
        CoordinatorDynamicConfig.class
    );
    Assert.assertEquals(ImmutableSet.of("test1", "test2"), actual.getSpecificDataSourcesToKillUnusedSegmentsIn());

    jsonStr = "{}";
    actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                CoordinatorDynamicConfig.class
            )
        ),
        CoordinatorDynamicConfig.class
    );
    Assert.assertEquals(ImmutableSet.of(), actual.getSpecificDataSourcesToKillUnusedSegmentsIn());
  }

  @Test
  public void testDynamicSerdeOfDataSourcesToNotKillStalePendingSegmentsIn() throws Exception
  {
    String jsonStr = "{\n" +
                     " \"killPendingSegmentsSkipList\": \"test1, test2\"\n" +
                     "}\n";
    CoordinatorDynamicConfig actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                CoordinatorDynamicConfig.class
            )
        ),
        CoordinatorDynamicConfig.class
    );
    Assert.assertEquals(ImmutableSet.of("test1", "test2"), actual.getDataSourcesToNotKillStalePendingSegmentsIn());

    jsonStr = "{\n" +
              " \"killPendingSegmentsSkipList\": [\"test1\", \"test2\"]\n" +
              "}\n";
    actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                CoordinatorDynamicConfig.class
            )
        ),
        CoordinatorDynamicConfig.class
    );
    Assert.assertEquals(ImmutableSet.of("test1", "test2"), actual.getDataSourcesToNotKillStalePendingSegmentsIn());

    jsonStr = "{}";
    actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                CoordinatorDynamicConfig.class
            )
        ),
        CoordinatorDynamicConfig.class
    );
    Assert.assertEquals(ImmutableSet.of(), actual.getDecommissioningNodes());
  }

  @Test
  public void testDynamicSerdeOfDecommissioningNodes() throws Exception
  {
    String jsonStr = "{\n" +
                     " \"decommissioningNodes\": \"test1, test2\"\n" +
                     "}\n";
    CoordinatorDynamicConfig actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                CoordinatorDynamicConfig.class
            )
        ),
        CoordinatorDynamicConfig.class
    );
    Assert.assertEquals(ImmutableSet.of("test1", "test2"), actual.getDecommissioningNodes());

    jsonStr = "{\n" +
              " \"decommissioningNodes\": [\"test1\", \"test2\"]\n" +
              "}\n";
    actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                CoordinatorDynamicConfig.class
            )
        ),
        CoordinatorDynamicConfig.class
    );
    Assert.assertEquals(ImmutableSet.of("test1", "test2"), actual.getDecommissioningNodes());

    jsonStr = "{}";
    actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                CoordinatorDynamicConfig.class
            )
        ),
        CoordinatorDynamicConfig.class
    );
    Assert.assertEquals(ImmutableSet.of(), actual.getDataSourcesToNotKillStalePendingSegmentsIn());
  }

  @Test
  public void testSerdeHandleNullableFields() throws Exception
  {
    String jsonStr = "{}";
    CoordinatorDynamicConfig actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                CoordinatorDynamicConfig.class
            )
        ),
        CoordinatorDynamicConfig.class
    );
    Assert.assertEquals(100.0, actual.getPercentOfSegmentsToConsiderPerMove(), 0.0);
    Assert.assertEquals(100.0, actual.getMaxSegmentsInNodeLoadingQueue(), 0.0);
    Assert.assertEquals(Integer.MAX_VALUE, actual.getMaxNonPrimaryReplicantsToLoad());
    Assert.assertEquals(Integer.MAX_VALUE, actual.getMaxSegmentsToLoadPerCoordinationCycle());
    Assert.assertEquals(false, actual.isUseRoundRobinSegmentAssignment());
  }

  @Test
  public void testIsKillUnusedSegmentsInAllDataSources() throws Exception
  {
    String jsonStr = "{\n" +
                     " \"killDataSourceWhitelist\": []\n" +
                     "}\n";
    CoordinatorDynamicConfig actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                CoordinatorDynamicConfig.class
            )
        ),
        CoordinatorDynamicConfig.class
    );
    Assert.assertTrue(actual.isKillUnusedSegmentsInAllDataSources());

    jsonStr = "{\n" +
              " \"killDataSourceWhitelist\": [\"test1\", \"test2\"]\n" +
              "}\n";
    actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                CoordinatorDynamicConfig.class
            )
        ),
        CoordinatorDynamicConfig.class
    );
    Assert.assertFalse(actual.isKillUnusedSegmentsInAllDataSources());
  }

  /**
   * This test is applicable for legacy configs that are removed between versions.
   *
   * If these unkown fields are included in the JSON payload for deserialization, they are ignored.
   * @throws Exception
   */
  @Test
  public void testUnknownFieldsHaveNoImpactOnDeserialization() throws Exception
  {
    String jsonStr = "{\n" +
                     " \"dummyField\": []\n" +
                     "}\n";
    CoordinatorDynamicConfig dummyActual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                CoordinatorDynamicConfig.class
            )
        ),
        CoordinatorDynamicConfig.class
    );
    jsonStr = "{}";
    CoordinatorDynamicConfig actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                CoordinatorDynamicConfig.class
            )
        ),
        CoordinatorDynamicConfig.class
    );
    Assert.assertEquals(actual, dummyActual);
  }

  /**
   * Confirms that the builder will honor values in CoordinatorDynamicConfig when it is used as the Defaults supplier.
   *
   * @throws Exception
   */
  @Test
  public void testBuilderBuildWithCoordinatorDynamicConfigDefaults() throws Exception
  {
    String jsonStr = "{\n" +
                     " \"decommissioningNodes\": \"test1, test2\"\n" +
                     "}\n";
    CoordinatorDynamicConfig defaults = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                CoordinatorDynamicConfig.class
            )
        ),
        CoordinatorDynamicConfig.class
    );
    CoordinatorDynamicConfig actual = CoordinatorDynamicConfig.builder().build(defaults);
    Assert.assertEquals(ImmutableSet.of("test1", "test2"), actual.getDecommissioningNodes());

  }
}
