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

package org.apache.druid.query.context;

import org.apache.druid.java.util.common.IAE;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class RequestHeaderContextConfigTest
{
  private final RequestHeaderContextConfig config =
      new RequestHeaderContextConfig(Map.of("X-Trace-Id", "traceId", "X-Correlation-Id", "correlationId"));

  @Test
  public void testCaptureAndReconstructHeaders()
  {
    final Map<String, String> captured = config.capture(header -> "X-Trace-Id".equals(header) ? "caller" : null);
    Assertions.assertEquals(Map.of("traceId", "caller"), captured);
    Assertions.assertEquals(
        Map.of("X-Trace-Id", "caller"),
        config.fromContext(Map.of("traceId", "caller", "unrelated", "ignored"))
    );
    Assertions.assertTrue(config.fromContext(Map.of("traceId", 42)).isEmpty());
  }

  @Test
  public void testInvalidHeaderValuesAreNotForwarded()
  {
    for (final String value : new String[]{"", " ", "line\rbreak", "line\nbreak", "null\u0000byte", "delete\u007f"}) {
      Assertions.assertTrue(config.capture(header -> value).isEmpty());
      Assertions.assertTrue(config.fromContext(Map.of("traceId", value)).isEmpty());
    }
    Assertions.assertEquals("trace-id", config.capture(header -> "trace-id").get("traceId"));
  }

  @Test
  public void testRejectInvalidMappings()
  {
    for (final String key : new String[]{"queryId", "sqlQueryId", "subQueryId", "timeout", ""}) {
      Assertions.assertThrows(IAE.class, () -> new RequestHeaderContextConfig(Map.of("X-Trace-Id", key)));
    }
    Assertions.assertThrows(IAE.class, () -> new RequestHeaderContextConfig(Map.of("Invalid Header", "traceId")));
    Assertions.assertThrows(IAE.class, () -> new RequestHeaderContextConfig(Map.of("X-A", "a", "x-a", "b")));
    Assertions.assertThrows(IAE.class, () -> new RequestHeaderContextConfig(Map.of("X-A", "same", "X-B", "same")));
  }
}
