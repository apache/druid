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

package org.apache.druid.indexing.kafka;

import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class ShareGroupIndexTaskIOConfigTest
{
  @Test
  public void testDefaultPollTimeout()
  {
    final ShareGroupIndexTaskIOConfig config = new ShareGroupIndexTaskIOConfig(
        "topic",
        "group",
        Map.of("bootstrap.servers", "localhost:9092"),
        new JsonInputFormat(null, null, null, null, null),
        null
    );
    Assertions.assertEquals(KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS, config.getPollTimeout());
  }
}
