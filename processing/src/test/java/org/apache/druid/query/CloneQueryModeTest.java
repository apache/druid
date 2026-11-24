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


package org.apache.druid.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CloneQueryModeTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerde() throws Exception
  {
    for (final CloneQueryMode mode : CloneQueryMode.values()) {
      final String json = JSON_MAPPER.writeValueAsString(mode);
      Assertions.assertEquals("\"" + mode + "\"", json);
      Assertions.assertEquals(mode, JSON_MAPPER.readValue(json, CloneQueryMode.class));
    }
    Assertions.assertEquals("\"preferClones\"", JSON_MAPPER.writeValueAsString(CloneQueryMode.PREFERCLONES));
  }

  @Test
  public void testFromStringRejectsUnknownValue()
  {
    Assertions.assertThrows(DruidException.class, () -> CloneQueryMode.fromString("unknown"));
  }
}
