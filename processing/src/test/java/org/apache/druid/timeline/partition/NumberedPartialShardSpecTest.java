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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class NumberedPartialShardSpecTest
{
  @Test
  public void testSerde() throws IOException
  {
    final NumberedPartialShardSpec expected = NumberedPartialShardSpec.instance();
    final ObjectMapper mapper = ShardSpecTestUtils.initObjectMapper();
    final byte[] json = mapper.writeValueAsBytes(expected);
    final PartialShardSpec fromJson = mapper.readValue(json, PartialShardSpec.class);
    Assert.assertSame(NumberedPartialShardSpec.class, fromJson.getClass());
  }

  @Test
  public void testComplete()
  {
    final NumberedPartialShardSpec partialShardSpec = NumberedPartialShardSpec.instance();
    final ShardSpec shardSpec = partialShardSpec.complete(new ObjectMapper(), 1, 3);
    Assert.assertEquals(new NumberedShardSpec(1, 3), shardSpec);
  }
}
