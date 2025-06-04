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

package org.apache.druid.server.coordinator;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class ServerCloneStatusTest
{
  @Test
  public void testSerde() throws Exception
  {
    ServerCloneStatus metrics = new ServerCloneStatus("host2", "host1", ServerCloneStatus.State.IN_PROGRESS, 3012, 10, 100);
    byte[] bytes = DefaultObjectMapper.INSTANCE.writeValueAsBytes(metrics);
    ServerCloneStatus deserialized = DefaultObjectMapper.INSTANCE.readValue(bytes, ServerCloneStatus.class);
    Assert.assertEquals(deserialized, metrics);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(ServerCloneStatus.class)
                  .withNonnullFields("sourceServer", "state")
                  .usingGetClass()
                  .verify();
  }
}
