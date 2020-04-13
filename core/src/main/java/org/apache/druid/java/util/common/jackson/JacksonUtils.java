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

package org.apache.druid.java.util.common.jackson;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public final class JacksonUtils
{
  public static final TypeReference<Map<String, Object>> TYPE_REFERENCE_MAP_STRING_OBJECT =
      new TypeReference<Map<String, Object>>()
      {
      };
  public static final TypeReference<Map<String, String>> TYPE_REFERENCE_MAP_STRING_STRING =
      new TypeReference<Map<String, String>>()
      {
      };
  public static final TypeReference<Map<String, Boolean>> TYPE_REFERENCE_MAP_STRING_BOOLEAN =
      new TypeReference<Map<String, Boolean>>()
      {
      };

  /** Silences Jackson's {@link IOException}. */
  public static <T> T readValue(ObjectMapper mapper, byte[] bytes, Class<T> valueClass)
  {
    try {
      return mapper.readValue(bytes, valueClass);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private JacksonUtils()
  {
  }
}
