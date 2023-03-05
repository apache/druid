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

package org.apache.druid.data.input.protobuf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JsonProvider;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import org.apache.druid.java.util.common.parsers.JSONFlattenerMaker;
import org.apache.druid.java.util.common.parsers.NotImplementedMappingProvider;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;

import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Basically a plain java object {@link ObjectFlatteners.FlattenerMaker}, but it lives here for now...
 */
public class ProtobufFlattenerMaker implements ObjectFlatteners.FlattenerMaker<Map<String, Object>>
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ProtobufJsonProvider JSON_PROVIDER = new ProtobufJsonProvider();

  private static final Configuration CONFIG = Configuration.builder()
                                                           .jsonProvider(JSON_PROVIDER)
                                                           .mappingProvider(new NotImplementedMappingProvider())
                                                           .options(EnumSet.of(Option.SUPPRESS_EXCEPTIONS))
                                                           .build();

  private final CharsetEncoder enc = StandardCharsets.UTF_8.newEncoder();

  private final boolean discoverNestedFields;

  public ProtobufFlattenerMaker(boolean discoverNestedFields)
  {
    this.discoverNestedFields = discoverNestedFields;
  }

  @Override
  public JsonProvider getJsonProvider()
  {
    return JSON_PROVIDER;
  }

  @Override
  public Iterable<String> discoverRootFields(Map<String, Object> obj)
  {
    // if discovering nested fields, just return all root fields since we want everything
    // else, we filter for literals and arrays of literals
    if (discoverNestedFields) {
      return obj.keySet();
    }
    Set<String> rootFields = Sets.newHashSetWithExpectedSize(obj.keySet().size());
    for (Map.Entry<String, Object> entry : obj.entrySet()) {
      if (entry.getValue() instanceof List || entry.getValue() instanceof Map) {
        continue;
      }
      rootFields.add(entry.getKey());
    }
    return rootFields;
  }

  @Override
  public Object getRootField(Map<String, Object> obj, String key)
  {
    return obj.get(key);
  }

  @Override
  public Function<Map<String, Object>, Object> makeJsonPathExtractor(String expr)
  {
    final JsonPath path = JsonPath.compile(expr);
    return map -> path.read(map, CONFIG);
  }

  @Override
  public Function<Map<String, Object>, Object> makeJsonQueryExtractor(String expr)
  {
    final JsonQuery jsonQuery;
    try {
      jsonQuery = JsonQuery.compile(expr);
    }
    catch (JsonQueryException e) {
      throw new RuntimeException(e);
    }
    return map -> {
      try {
        return JSONFlattenerMaker.convertJsonNode(
            jsonQuery.apply((JsonNode) OBJECT_MAPPER.valueToTree(map)).get(0),
            enc
        );
      }
      catch (JsonQueryException e) {
        throw new RuntimeException(e);
      }
    };
  }
}
