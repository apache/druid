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

package org.apache.druid.java.util.common.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

/**
 * JSON parser class that uses the JsonPath library to access fields via path expressions.
 */
public class JSONPathParser implements Parser<String, Object>
{
  private final ObjectMapper mapper;
  private final ObjectFlattener<JsonNode> flattener;

  /**
   * Constructor
   *
   * @param flattenSpec Provide a path spec for flattening and field discovery.
   * @param mapper      Optionally provide an ObjectMapper, used by the parser for reading the input JSON.
   */
  public JSONPathParser(JSONPathSpec flattenSpec, ObjectMapper mapper)
  {
    this.mapper = mapper == null ? new ObjectMapper() : mapper;
    this.flattener = ObjectFlatteners.create(flattenSpec, new JSONFlattenerMaker());
  }

  @Override
  public List<String> getFieldNames()
  {
    return null;
  }

  @Override
  public void setFieldNames(Iterable<String> fieldNames)
  {
  }

  /**
   * @param input JSON string. The root must be a JSON object, not an array.
   *              e.g., {"valid": "true"} and {"valid":[1,2,3]} are supported
   *              but [{"invalid": "true"}] and [1,2,3] are not.
   *
   * @return A map of field names and values
   */
  @Override
  public Map<String, Object> parseToMap(String input)
  {
    try {
      JsonNode document = mapper.readValue(input, JsonNode.class);
      return flattener.flatten(document);
    }
    catch (Exception e) {
      throw new ParseException(e, "Unable to parse row [%s]", input);
    }
  }
}
