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

package org.apache.druid.indexing.overlord.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TaskSlotLimitsDeserializer extends JsonDeserializer<Map<String, Number>>
{

  @Override
  public Map<String, Number> deserialize(JsonParser p, DeserializationContext ctxt)
      throws IOException
  {
    Map<String, Number> taskSlotLimits = new HashMap<>();

    JsonNode node = p.getCodec().readTree(p);
    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();

    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      String key = entry.getKey();
      JsonNode valueNode = entry.getValue();

      if (valueNode.isDouble()) {
        double value = valueNode.asDouble();
        if (value < 0 || value > 1) {
          throw new IllegalArgumentException(
              "Limit value for task type '" + key + "' must be between 0 and 1 for ratios.");
        }
        taskSlotLimits.put(key, value);
      } else if (valueNode.isInt()) {
        int value = valueNode.asInt();
        if (value < 0) {
          throw new IllegalArgumentException(
              "Limit value for task type '" + key + "' must be >= 0 for integer limits.");
        }
        taskSlotLimits.put(key, value);
      } else {
        throw new IllegalArgumentException(
            "Limit value for task type '" + key + "' must be a number (either Double or Integer).");
      }
    }
    return taskSlotLimits;
  }
}
