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

/** Accepts legacy JSON millisecond numbers and duration strings. */
public class HumanReadableDurationDeserializer extends JsonDeserializer<HumanReadableDuration>
{
  @Override
  public HumanReadableDuration deserialize(JsonParser parser, DeserializationContext context)
      throws IOException
  {
    try {
      if (parser.currentToken() == JsonToken.VALUE_NUMBER_INT) {
        return HumanReadableDuration.ofMillis(parser.getLongValue());
      }
      if (parser.currentToken() == JsonToken.VALUE_STRING) {
        return HumanReadableDuration.parse(parser.getText());
      }
    }
    catch (IAE e) {
      return context.reportInputMismatch(HumanReadableDuration.class, "%s", e.getMessage());
    }
    return (HumanReadableDuration) context.handleUnexpectedToken(HumanReadableDuration.class, parser);
  }
}
