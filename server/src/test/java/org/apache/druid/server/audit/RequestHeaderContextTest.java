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

package org.apache.druid.server.audit;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class RequestHeaderContextTest
{
  @AfterEach
  public void tearDown()
  {
    RequestHeaderContext.clear();
  }

  @Test
  public void testCurrentEmptyByDefault()
  {
    Assertions.assertTrue(RequestHeaderContext.current().isEmpty());
  }

  @Test
  public void testBindAndCurrent()
  {
    RequestHeaderContext.bind(ImmutableMap.of("traceId", "abc-123", "foo", "bar"));
    Assertions.assertEquals(2, RequestHeaderContext.current().size());
    Assertions.assertEquals("abc-123", RequestHeaderContext.current().get("traceId"));
    Assertions.assertEquals("bar", RequestHeaderContext.current().get("foo"));
  }

  @Test
  public void testBindEmptyTreatedAsClear()
  {
    RequestHeaderContext.bind(ImmutableMap.of("traceId", "abc"));
    RequestHeaderContext.bind(Collections.emptyMap());
    Assertions.assertTrue(RequestHeaderContext.current().isEmpty());
  }

  @Test
  public void testBindNullTreatedAsClear()
  {
    RequestHeaderContext.bind(ImmutableMap.of("traceId", "abc"));
    RequestHeaderContext.bind(null);
    Assertions.assertTrue(RequestHeaderContext.current().isEmpty());
  }

  @Test
  public void testClearRemoves()
  {
    RequestHeaderContext.bind(ImmutableMap.of("traceId", "abc"));
    RequestHeaderContext.clear();
    Assertions.assertTrue(RequestHeaderContext.current().isEmpty());
  }

  @Test
  public void testCurrentReturnsImmutableSnapshot()
  {
    RequestHeaderContext.bind(ImmutableMap.of("traceId", "abc"));
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> RequestHeaderContext.current().put("evil", "value")
    );
  }
}
