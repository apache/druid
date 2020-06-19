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

package org.apache.druid.timeline.partition;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Assert;
import org.junit.Test;

public class BuildingNumberedShardSpecTest
{
  @Test
  public void testConvert()
  {
    Assert.assertEquals(new NumberedShardSpec(5, 10), new BuildingNumberedShardSpec(5).convert(10));
  }

  @Test
  public void testCreateChunk()
  {
    Assert.assertEquals(
        new NumberedPartitionChunk<>(5, 0, "test"),
        new BuildingNumberedShardSpec(5).createChunk("test")
    );
  }

  @Test
  public void testSerde() throws JsonProcessingException
  {
    final ObjectMapper mapper = ShardSpecTestUtils.initObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.registerSubtypes(new NamedType(BuildingNumberedShardSpec.class, BuildingNumberedShardSpec.TYPE));
    final BuildingNumberedShardSpec original = new BuildingNumberedShardSpec(5);
    final String json = mapper.writeValueAsString(original);
    final BuildingNumberedShardSpec fromJson = (BuildingNumberedShardSpec) mapper.readValue(json, ShardSpec.class);
    Assert.assertEquals(original, fromJson);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(BuildingNumberedShardSpec.class).usingGetClass().verify();
  }
}
