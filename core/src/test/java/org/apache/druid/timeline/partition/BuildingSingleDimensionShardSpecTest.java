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

import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import org.apache.druid.java.util.common.ISE;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class BuildingSingleDimensionShardSpecTest
{
  private static final ObjectMapper OBJECT_MAPPER = setupObjectMapper();

  @Test
  public void testConvert()
  {
    Assert.assertEquals(
        new SingleDimensionShardSpec("dim", "start", "end", 5, 10),
        new BuildingSingleDimensionShardSpec(1, "dim", "start", "end", 5).convert(10)
    );
  }

  @Test
  public void testCreateChunk()
  {
    Assert.assertEquals(
        new NumberedPartitionChunk<>(5, 0, "test"),
        new BuildingSingleDimensionShardSpec(1, "dim", "start", "end", 5).createChunk("test")
    );
  }

  @Test
  public void testSerde()
  {
    final BuildingSingleDimensionShardSpec original =
        new BuildingSingleDimensionShardSpec(1, "dim", "start", "end", 5);
    final String json = serialize(original);
    ShardSpec shardSpec = deserialize(json, ShardSpec.class);
    Assert.assertEquals(ShardSpec.Type.BUILDING_SINGLE_DIM, shardSpec.getType());

    final BuildingSingleDimensionShardSpec fromJson = (BuildingSingleDimensionShardSpec) shardSpec;
    Assert.assertEquals(original, fromJson);
  }

  @Test
  public void testGetSerializableObject()
  {
    BuildingSingleDimensionShardSpec shardSpec =
        new BuildingSingleDimensionShardSpec(1, "dim", "abc", "xyz", 5);

    // Verify the fields of the serializable object
    Map<String, Object> jsonMap = shardSpec.getSerializableObject();
    Assert.assertEquals(5, jsonMap.size());
    Assert.assertEquals(1, jsonMap.get("bucketId"));
    Assert.assertEquals("dim", jsonMap.get("dimension"));
    Assert.assertEquals("abc", jsonMap.get("start"));
    Assert.assertEquals("xyz", jsonMap.get("end"));
    Assert.assertEquals(5, jsonMap.get("partitionNum"));
  }

  @Test
  public void testDeserializeFromMap()
  {
    final String json = "{\"type\": \"" + ShardSpec.Type.BUILDING_SINGLE_DIM + "\","
                        + " \"bucketId\":1,"
                        + " \"dimension\": \"dim\","
                        + " \"start\": \"abc\","
                        + " \"end\": \"xyz\","
                        + " \"partitionNum\": 5}";

    BuildingSingleDimensionShardSpec shardSpec =
        (BuildingSingleDimensionShardSpec) deserialize(json, ShardSpec.class);
    Assert.assertEquals(
        new BuildingSingleDimensionShardSpec(1, "dim", "abc", "xyz", 5),
        shardSpec
    );
  }

  @Test
  public void testEquals()
  {
    Assert.assertEquals(
        new BuildingSingleDimensionShardSpec(10, "dim", "start", "end", 4),
        new BuildingSingleDimensionShardSpec(10, "dim", "start", "end", 4)
    );
  }

  private static ObjectMapper setupObjectMapper()
  {
    final ObjectMapper mapper = ShardSpecTestUtils.initObjectMapper();
    mapper.registerSubtypes(
        new NamedType(BuildingSingleDimensionShardSpec.class, ShardSpec.Type.BUILDING_SINGLE_DIM)
    );
    mapper.setInjectableValues(new Std().addValue(ObjectMapper.class, mapper));

    return mapper;
  }

  private String serialize(Object object)
  {
    try {
      return OBJECT_MAPPER.writeValueAsString(object);
    }
    catch (Exception e) {
      throw new ISE("Error while serializing");
    }
  }

  private <T> T deserialize(String json, Class<T> clazz)
  {
    try {
      return OBJECT_MAPPER.readValue(json, clazz);
    }
    catch (Exception e) {
      throw new ISE(e, "Error while deserializing");
    }
  }

}
