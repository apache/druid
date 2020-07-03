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
import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Assert;
import org.junit.Test;

public class BuildingSingleDimensionShardSpecTest
{
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
  public void testSerde() throws JsonProcessingException
  {
    final ObjectMapper mapper = ShardSpecTestUtils.initObjectMapper();
    mapper.registerSubtypes(
        new NamedType(BuildingSingleDimensionShardSpec.class, BuildingSingleDimensionShardSpec.TYPE)
    );
    mapper.setInjectableValues(new Std().addValue(ObjectMapper.class, mapper));
    final BuildingSingleDimensionShardSpec original = new BuildingSingleDimensionShardSpec(1, "dim", "start", "end", 5);
    final String json = mapper.writeValueAsString(original);
    final BuildingSingleDimensionShardSpec fromJson = (BuildingSingleDimensionShardSpec) mapper.readValue(
        json,
        ShardSpec.class
    );
    Assert.assertEquals(original, fromJson);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(BuildingSingleDimensionShardSpec.class).usingGetClass().verify();
  }
}
