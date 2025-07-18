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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class SegmentLoadingCapabilitiesTest
{
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testSerde() throws Exception
  {
    SegmentLoadingCapabilities capabilities = new SegmentLoadingCapabilities(1, 4);

    SegmentLoadingCapabilities reread = jsonMapper.readValue(jsonMapper.writeValueAsString(capabilities), SegmentLoadingCapabilities.class);

    Assert.assertEquals(capabilities.getNumLoadingThreads(), reread.getNumLoadingThreads());
    Assert.assertEquals(capabilities.getNumTurboLoadingThreads(), reread.getNumTurboLoadingThreads());
  }

  @Test
  public void testSerdeFromJson() throws JsonProcessingException
  {
    String json = "{\"numLoadingThreads\":3,\"numTurboLoadingThreads\":5}";
    SegmentLoadingCapabilities reread = jsonMapper.readValue(json, SegmentLoadingCapabilities.class);

    Assert.assertEquals(3, reread.getNumLoadingThreads());
    Assert.assertEquals(5, reread.getNumTurboLoadingThreads());
  }
}
