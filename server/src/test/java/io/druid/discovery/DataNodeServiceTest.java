/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.discovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.segment.TestHelper;
import io.druid.server.coordination.ServerType;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class DataNodeServiceTest
{
  @Test
  public void testSerde() throws Exception
  {
    DruidService expected = new DataNodeService(
        "tier",
        100,
        ServerType.HISTORICAL,
        1
    );

    ObjectMapper mapper = TestHelper.makeJsonMapper();
    DruidService actual = mapper.readValue(
        mapper.writeValueAsString(expected),
        DruidService.class
    );

    Assert.assertEquals(expected, actual);
  }
}
