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

package org.apache.druid.java.util.emitter.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class EventMapTest
{
  @Test
  public void testSerializedEventMapIsOrdered() throws JsonProcessingException
  {
    final EventMap map = EventMap.builder()
                                 .put("k1", "v1")
                                 .put("k2", "v2")
                                 .put("k3", "v3")
                                 .put("k4", "v4")
                                 .put("k5", "v5")
                                 .put("k0", "v0")
                                 .put("a0", "a0")
                                 .put("x0", "x0")
                                 .build();

    Assert.assertEquals(
        "{\"k1\":\"v1\",\"k2\":\"v2\",\"k3\":\"v3\",\"k4\":\"v4\",\"k5\":\"v5\",\"k0\":\"v0\",\"a0\":\"a0\",\"x0\":\"x0\"}",
        new DefaultObjectMapper().writeValueAsString(map)
    );
  }
}