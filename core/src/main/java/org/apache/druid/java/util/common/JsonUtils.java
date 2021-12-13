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

package org.apache.druid.java.util.common;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonMappingException;

import java.io.IOException;

public class JsonUtils
{
  private JsonUtils()
  {
  }

  /**
   * Skip over a single JSON value: scalar or composite.
   */
  public static void skipValue(final JsonParser jp, String key) throws IOException
  {
    final JsonToken token = jp.currentToken();
    switch (token) {
      case START_OBJECT:
      case START_ARRAY:
        jp.skipChildren();
        jp.nextToken();
        break;
      default:
        if (token.isScalarValue()) {
          jp.nextToken();
          return;
        }
        throw new JsonMappingException(jp, "Invalid JSON inside unknown key: " + key);
    }
  }
}
