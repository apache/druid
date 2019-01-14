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
import com.google.common.collect.FluentIterable;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import org.apache.druid.data.input.impl.FastJacksonJsonNodeJsonProvider;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class JSONFlattenerMaker implements ObjectFlatteners.FlattenerMaker<JsonNode>
{
  private static final Configuration JSONPATH_CONFIGURATION =
      Configuration.builder()
                   .jsonProvider(new FastJacksonJsonNodeJsonProvider())
                   .mappingProvider(new JacksonMappingProvider())
                   .options(EnumSet.of(Option.SUPPRESS_EXCEPTIONS))
                   .build();

  private final CharsetEncoder enc = StandardCharsets.UTF_8.newEncoder();

  @Override
  public Iterable<String> discoverRootFields(final JsonNode obj)
  {
    return FluentIterable.from(() -> obj.fields())
                         .filter(
                             entry -> {
                               final JsonNode val = entry.getValue();
                               return !(val.isObject() || val.isNull() || (val.isArray() && !isFlatList(val)));
                             }
                         )
                         .transform(Map.Entry::getKey);
  }

  @Override
  public Object getRootField(final JsonNode obj, final String key)
  {
    return valueConversionFunction(obj.get(key));
  }

  @Override
  public Function<JsonNode, Object> makeJsonPathExtractor(final String expr)
  {
    final JsonPath jsonPath = JsonPath.compile(expr);
    return node -> valueConversionFunction(jsonPath.read(node, JSONPATH_CONFIGURATION));
  }

  @Override
  public Function<JsonNode, Object> makeJsonQueryExtractor(final String expr)
  {
    try {
      final JsonQuery jsonQuery = JsonQuery.compile(expr);
      return jsonNode -> {
        try {
          return valueConversionFunction(jsonQuery.apply(jsonNode).get(0));
        }
        catch (JsonQueryException e) {
          throw new RuntimeException(e);
        }
      };
    }
    catch (JsonQueryException e) {
      throw new RuntimeException(e);
    }
  }

  @Nullable
  private Object valueConversionFunction(JsonNode val)
  {
    if (val == null || val.isNull()) {
      return null;
    }

    if (val.isInt() || val.isLong()) {
      return val.asLong();
    }

    if (val.isNumber()) {
      return val.asDouble();
    }

    if (val.isTextual()) {
      return charsetFix(val.asText());
    }

    if (val.isArray()) {
      List<Object> newList = new ArrayList<>();
      for (JsonNode entry : val) {
        if (!entry.isNull()) {
          newList.add(valueConversionFunction(entry));
        }
      }
      return newList;
    }

    if (val.isObject()) {
      Map<String, Object> newMap = new LinkedHashMap<>();
      for (Iterator<Map.Entry<String, JsonNode>> it = val.fields(); it.hasNext(); ) {
        Map.Entry<String, JsonNode> entry = it.next();
        newMap.put(entry.getKey(), valueConversionFunction(entry.getValue()));
      }
      return newMap;
    }

    return val;
  }

  @Nullable
  private String charsetFix(String s)
  {
    if (s != null && !enc.canEncode(s)) {
      // Some whacky characters are in this string (e.g. \uD900). These are problematic because they are decodeable
      // by new String(...) but will not encode into the same character. This dance here will replace these
      // characters with something more sane.
      return StringUtils.fromUtf8(StringUtils.toUtf8(s));
    } else {
      return s;
    }
  }

  private boolean isFlatList(JsonNode list)
  {
    for (JsonNode obj : list) {
      if (obj.isObject() || obj.isArray()) {
        return false;
      }
    }
    return true;
  }
}
