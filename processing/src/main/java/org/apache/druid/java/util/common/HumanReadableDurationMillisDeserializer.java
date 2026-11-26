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
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Deserializes a non-negative duration into a millisecond {@code long}.
 * Unlike {@link HumanReadableDuration}, unsuffixed milliseconds retain the full {@code long} range.
 */
public class HumanReadableDurationMillisDeserializer extends JsonDeserializer<Long>
{
  private static final Pattern BARE_NUMBER = Pattern.compile("[0-9]+");

  @Override
  public Long deserialize(JsonParser parser, DeserializationContext context) throws IOException
  {
    if (parser.currentToken() == JsonToken.VALUE_NUMBER_INT) {
      return parser.getLongValue();
    }
    if (parser.currentToken() == JsonToken.VALUE_STRING) {
      final String value = parser.getText().trim();
      try {
        if (BARE_NUMBER.matcher(value).matches()) {
          return Long.parseLong(value);
        }
        return HumanReadableDuration.parse(value).getMillis();
      }
      catch (IllegalArgumentException e) {
        return context.reportInputMismatch(Long.class, "Invalid duration [%s]", value);
      }
    }
    return (Long) context.handleUnexpectedToken(Long.class, parser);
  }
}
