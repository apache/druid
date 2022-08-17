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

package org.apache.druid.emitter.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.druid.java.util.emitter.core.Event;

import java.util.HashMap;
import java.util.Map;

/**
 * Serializes the any event to a JSON string prepresntation
 */
public class EventToJsonSerializer
{
  private final ObjectMapper objectMapper;
  private final Map<String, Object> additionalProperties;

  /**
   * Returns serializer without additional properties
   */
  public static EventToJsonSerializer of(final ObjectMapper objectMapper)
  {
    return new EventToJsonSerializer(objectMapper, new HashMap<>());
  }

  /**
   * Constructs serializer with given ObjectMapper and additional properties
   *
   * @param objectMapper         - will be used for serializaing data to JSON String
   * @param additionalProperties - additional properties that will be added to the output JSON
   */
  public EventToJsonSerializer(final ObjectMapper objectMapper, final Map<String, Object> additionalProperties)
  {
    this.objectMapper = objectMapper;
    this.additionalProperties = additionalProperties;
  }

  /**
   * Adds the value to the additional properties if the value is not null
   */
  public EventToJsonSerializer withProperty(String name, Object value)
  {
    if (value != null) {
      additionalProperties.put(name, value);
    }
    return this;
  }

  /**
   * Serializes Event to a JSON String representation
   */
  public String serialize(Event event) throws JsonProcessingException
  {
    JsonBuilder builder = new JsonBuilder(objectMapper);

    event.toMap().forEach(builder::putKeyValue);
    additionalProperties.forEach(builder::putKeyValue);

    return objectMapper.writeValueAsString(builder.build());
  }

  private static class JsonBuilder
  {
    private final ObjectNode json;
    private final ObjectMapper objectMapper;

    private JsonBuilder(final ObjectMapper objectMapper)
    {
      this.json = objectMapper.createObjectNode();
      this.objectMapper = objectMapper;
    }

    private void putKeyValue(String key, Object value)
    {
      json.set(key, objectMapper.valueToTree(value));
    }

    private ObjectNode build()
    {
      return json;
    }
  }
}
