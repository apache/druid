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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;

/**
 * Deserialize a response context. The response context is created for single-thread use.
 * (That is, it is non-concurrent.) Clients of this code should convert the
 * context to concurrent if it will be used across threads.
 */
@SuppressWarnings("serial")
public class ResponseContextDeserializer extends StdDeserializer<ResponseContext>
{
  public ResponseContextDeserializer()
  {
    super(ResponseContext.class);
  }

  @Override
  public ResponseContext deserialize(
      final JsonParser jp,
      final DeserializationContext ctxt
  ) throws IOException
  {
    if (jp.currentToken() != JsonToken.START_OBJECT) {
      throw ctxt.wrongTokenException(jp, ResponseContext.class, JsonToken.START_OBJECT, null);
    }

    final ResponseContext retVal = ResponseContext.createEmpty();

    jp.nextToken();

    ResponseContext.Keys keys = ResponseContext.Keys.instance();
    while (jp.currentToken() == JsonToken.FIELD_NAME) {
      // Get the key. Since this is a deserialization, the sender may
      // be a different version of Druid with a different set of keys.
      // Ignore any keys which the sender knows about but this node
      // does not know about.
      final ResponseContext.Key key = keys.find(jp.getText());

      jp.nextToken();
      if (key == null) {
        skipValue(jp, jp.getText());
      } else {
        retVal.add(key, key.readValue(jp));
      }

      jp.nextToken();
    }

    if (jp.currentToken() != JsonToken.END_OBJECT) {
      throw ctxt.wrongTokenException(jp, ResponseContext.class, JsonToken.END_OBJECT, null);
    }

    return retVal;
  }

  /**
   * Skip over a single JSON value: scalar or composite.
   */
  private void skipValue(final JsonParser jp, String key) throws IOException
  {
    final JsonToken token = jp.currentToken();
    switch (token) {
      case START_OBJECT:
        skipTo(jp, JsonToken.END_OBJECT);
        break;
      case START_ARRAY:
        skipTo(jp, JsonToken.END_ARRAY);
        break;
      default:
        if (token.isScalarValue()) {
          return;
        }
        throw new JsonMappingException(jp, "Invalid JSON inside unknown key: " + key);
    }
  }

  /**
   * Freewheel over the contents of a structured object, including any
   * nested structured objects, until the given end token.
   */
  private void skipTo(final JsonParser jp, JsonToken end) throws IOException
  {
    while (true) {
      jp.nextToken();
      final JsonToken token = jp.currentToken();
      if (token == null) {
        throw new JsonMappingException(jp, "Premature EOF");
      }
      switch (token) {
        case START_OBJECT:
          skipTo(jp, JsonToken.END_OBJECT);
          break;
        case START_ARRAY:
          skipTo(jp, JsonToken.END_ARRAY);
          break;
        default:
          if (token == end) {
            return;
          }
      }
    }
  }
}
