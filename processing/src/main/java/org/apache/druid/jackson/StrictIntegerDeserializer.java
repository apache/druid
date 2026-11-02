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

package org.apache.druid.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

/**
 * Deserializes an {@link Integer} from a JSON integer or a string holding an integer, such as {@code 1} or
 * {@code "1"}. Unlike Jackson's default, it rejects values that would be silently truncated, such as {@code 0.5} and
 * {@code 1.0}.
 * <p>
 * Use it with {@code @JsonDeserialize(using = ...)} on a field, or {@code contentUsing = ...} on a collection.
 */
public class StrictIntegerDeserializer extends JsonDeserializer<Integer>
{
  @Override
  public Integer deserialize(final JsonParser parser, final DeserializationContext context) throws IOException
  {
    if (parser.hasToken(JsonToken.VALUE_NUMBER_INT)) {
      return parser.getIntValue();
    }
    if (parser.hasToken(JsonToken.VALUE_STRING)) {
      try {
        return Integer.parseInt(parser.getText());
      }
      catch (NumberFormatException ignored) {
        // Fall through to report the offending value.
      }
    }
    return context.reportInputMismatch(
        Integer.class,
        "Expected a JSON integer but got %s [%s]",
        parser.currentToken(),
        parser.getText()
    );
  }
}
