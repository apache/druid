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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class NoneShardSpecTest
{
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testEqualsAndHashCode()
  {
    final ShardSpec one = NoneShardSpec.instance();
    final ShardSpec two = NoneShardSpec.instance();
    Assert.assertEquals(one, two);
    Assert.assertEquals(one.hashCode(), two.hashCode());
  }

  @Test
  public void testSerde() throws Exception
  {
    final NoneShardSpec one = NoneShardSpec.instance();
    NoneShardSpec serde1 = jsonMapper.readValue(jsonMapper.writeValueAsString(one), NoneShardSpec.class);
    NoneShardSpec serde2 = jsonMapper.readValue(jsonMapper.writeValueAsString(one), NoneShardSpec.class);

    // Serde should return same object instead of creating new one every time.
    Assert.assertTrue(serde1 == serde2);
    Assert.assertTrue(one == serde1);
    Assert.assertEquals(ShardSpec.Type.NONE, serde1.getType());
  }

  @Test
  public void testPartitionFieldIgnored() throws IOException
  {
    final String jsonStr = "{\"type\": \"none\",\"partitionNum\": 2}";
    final ShardSpec noneShardSpec = jsonMapper.readValue(jsonStr, ShardSpec.class);
    Assert.assertEquals(NoneShardSpec.instance(), noneShardSpec);
  }
}
