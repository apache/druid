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
import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Assert;
import org.junit.Test;

public class BuildingHashBasedNumberedShardSpecTest
{
  private final ObjectMapper mapper = ShardSpecTestUtils.initObjectMapper();

  @Test
  public void testConvert()
  {
    Assert.assertEquals(
        new HashBasedNumberedShardSpec(5, 10, 5, 12, ImmutableList.of("dim"), mapper),
        new BuildingHashBasedNumberedShardSpec(5, 5, 12, ImmutableList.of("dim"), mapper).convert(10)
    );
  }

  @Test
  public void testCreateChunk()
  {
    Assert.assertEquals(
        new NumberedPartitionChunk<>(5, 0, "test"),
        new BuildingHashBasedNumberedShardSpec(5, 5, 12, ImmutableList.of("dim"), mapper)
            .createChunk("test")
    );
  }

  @Test
  public void testSerde() throws JsonProcessingException
  {
    mapper.registerSubtypes(
        new NamedType(BuildingHashBasedNumberedShardSpec.class, BuildingHashBasedNumberedShardSpec.TYPE)
    );
    mapper.setInjectableValues(new Std().addValue(ObjectMapper.class, mapper));
    final BuildingHashBasedNumberedShardSpec original = new BuildingHashBasedNumberedShardSpec(
        3,
        5,
        12,
        ImmutableList.of("dim"),
        mapper
    );
    final String json = mapper.writeValueAsString(original);
    final BuildingHashBasedNumberedShardSpec fromJson = (BuildingHashBasedNumberedShardSpec) mapper.readValue(
        json,
        ShardSpec.class
    );
    Assert.assertEquals(original, fromJson);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(BuildingHashBasedNumberedShardSpec.class)
                  .withIgnoredFields("jsonMapper")
                  .withPrefabValues(ObjectMapper.class, new ObjectMapper(), new ObjectMapper())
                  .usingGetClass()
                  .verify();
  }
}
