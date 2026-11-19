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
import org.apache.druid.java.util.common.jackson.JacksonUtils;

import java.io.IOException;
import java.util.Map;

public class QueryContextTestUtils
{
  private QueryContextTestUtils()
  {
  }

  /**
   * Returns a query context as it arrives from a client: serialized to JSON and read back. Typed values that tests put
   * into a context, such as enums, become their JSON form, so that tests also exercise how parameters parse it.
   */
  public static Map<String, Object> asJsonContext(final ObjectMapper jsonMapper, final Map<String, Object> context)
  {
    try {
      return jsonMapper.readValue(
          jsonMapper.writeValueAsBytes(context),
          JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
