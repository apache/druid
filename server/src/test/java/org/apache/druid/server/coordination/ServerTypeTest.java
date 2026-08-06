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

package org.apache.druid.server.coordination;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class ServerTypeTest
{
  @Test
  public void testAssignable()
  {
    Assertions.assertTrue(ServerType.HISTORICAL.isSegmentReplicationTarget());
    Assertions.assertTrue(ServerType.BRIDGE.isSegmentReplicationTarget());
    Assertions.assertFalse(ServerType.REALTIME.isSegmentReplicationTarget());
    Assertions.assertFalse(ServerType.INDEXER_EXECUTOR.isSegmentReplicationTarget());
  }

  @Test
  public void testFromString()
  {
    Assertions.assertEquals(ServerType.HISTORICAL, ServerType.fromString("historical"));
    Assertions.assertEquals(ServerType.BRIDGE, ServerType.fromString("bridge"));
    Assertions.assertEquals(ServerType.REALTIME, ServerType.fromString("realtime"));
    Assertions.assertEquals(ServerType.INDEXER_EXECUTOR, ServerType.fromString("indexer-executor"));
  }

  @Test
  public void testToString()
  {
    Assertions.assertEquals(ServerType.HISTORICAL.toString(), "historical");
    Assertions.assertEquals(ServerType.BRIDGE.toString(), "bridge");
    Assertions.assertEquals(ServerType.REALTIME.toString(), "realtime");
    Assertions.assertEquals(ServerType.INDEXER_EXECUTOR.toString(), "indexer-executor");
  }

  @Test
  public void testInvalidName()
  {
    org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () ->
      ServerType.fromString("invalid"));
  }
}
