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
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.google.common.collect.FluentIterable;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JsonProvider;
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
  private static final JsonProvider JSON_PROVIDER = new FastJacksonJsonNodeJsonProvider();

  private static final Configuration JSONPATH_CONFIGURATION =
      Configuration.builder()
                   .jsonProvider(JSON_PROVIDER)
                   .mappingProvider(new JacksonMappingProvider())
                   .options(EnumSet.of(Option.SUPPRESS_EXCEPTIONS))
                   .build();

  private final CharsetEncoder enc = StandardCharsets.UTF_8.newEncoder();
  private final boolean keepNullValues;

  private final boolean discoverNestedFields;


  public JSONFlattenerMaker(boolean keepNullValues, boolean discoverNestedFields)
  {
    this.keepNullValues = keepNullValues;
    this.discoverNestedFields = discoverNestedFields;
  }

  @Override
  public Iterable<String> discoverRootFields(final JsonNode obj)
  {
    // if discovering nested fields, just return all root fields since we want everything
    // else, we filter for literals and arrays of literals
    if (discoverNestedFields) {
      return obj::fieldNames;
    }
    return FluentIterable.from(obj::fields)
                         .filter(
                             entry -> {
                               final JsonNode val = entry.getValue();
                               // If the keepNullValues is set on the JSONParseSpec then null values should not be filtered out
                               return !(val.isObject() || (!keepNullValues && val.isNull()) || (val.isArray() && !isFlatList(val)));
                             }
                         )
                         .transform(Map.Entry::getKey);
  }

  @Override
  public Object getRootField(final JsonNode obj, final String key)
  {
    return finalizeConversionForMap(obj.get(key));
  }

  @Override
  public Function<JsonNode, Object> makeJsonPathExtractor(final String expr)
  {
    final JsonPath jsonPath = JsonPath.compile(expr);
    return node -> finalizeConversionForMap(jsonPath.read(node, JSONPATH_CONFIGURATION));
  }

  @Override
  public Function<JsonNode, Object> makeJsonQueryExtractor(final String expr)
  {
    try {
      final JsonQuery jsonQuery = JsonQuery.compile(expr);
      return jsonNode -> {
        try {
          return finalizeConversionForMap(jsonQuery.apply(jsonNode).get(0));
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

  @Override
  public Function<JsonNode, Object> makeJsonTreeExtractor(final List<String> nodes)
  {
    // create a defensive copy
    final String[] keyNames = nodes.toArray(new String[0]);

    return jsonNode -> {
      JsonNode targetNode = jsonNode;
      for (String keyName : keyNames) {
        if (targetNode == null) {
          return null;
        }
        targetNode = targetNode.get(keyName);
      }
      return finalizeConversionForMap(targetNode);
    };
  }

  @Override
  public JsonProvider getJsonProvider()
  {
    return JSON_PROVIDER;
  }

  @Override
  public Object finalizeConversionForMap(Object o)
  {
    if (o instanceof JsonNode) {
      return convertJsonNode((JsonNode) o, enc);
    }
    return o;
  }

  @Nullable
  public static Object convertJsonNode(JsonNode val, CharsetEncoder enc)
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
      return charsetFix(val.asText(), enc);
    }

    if (val.isBoolean()) {
      return val.asBoolean();
    }

    // this is a jackson specific type, and is unlikely to occur in the wild. But, in the event we do encounter it,
    // handle it since it is a ValueNode
    if (val.isBinary() && val instanceof BinaryNode) {
      return ((BinaryNode) val).binaryValue();
    }


    if (val.isArray()) {
      List<Object> newList = new ArrayList<>();
      for (JsonNode entry : val) {
        newList.add(convertJsonNode(entry, enc));
      }
      return newList;
    }

    if (val.isObject()) {
      Map<String, Object> newMap = new LinkedHashMap<>();
      for (Iterator<Map.Entry<String, JsonNode>> it = val.fields(); it.hasNext(); ) {
        Map.Entry<String, JsonNode> entry = it.next();
        newMap.put(entry.getKey(), convertJsonNode(entry.getValue(), enc));
      }
      return newMap;
    }

    // All ValueNode implementations, as well as ArrayNode and ObjectNode will be handled by this point, so we should
    // only be dealing with jackson specific types if we end up here (MissingNode, POJONode) so we can just return null
    // so that we don't leak unhadled JsonNode objects
    return null;
  }

  /**
   * Fix up a string so it can round-trip through UTF-8 encoding without loss of fidelity. This comes up when a string
   * has surrogates (e.g. \uD900) that appear in invalid positions, such as alone rather than as part of a
   * surrogate pair.
   *
   * This operation is useful because when a string cannot round-trip properly, it can cause it to sort differently
   * relative to other strings after an encode/decode round-trip. This causes incorrect ordering in cases where strings
   * are sorted, then encoded, then decoded again.
   *
   * @param s   string, or null
   * @param enc UTF-8 encoder
   *
   * @return the original string, or a fixed version that can round-trip properly
   */
  @Nullable
  static String charsetFix(@Nullable String s, CharsetEncoder enc)
  {
    if (s != null && !isBmp(s) && !enc.canEncode(s)) {
      // Note: the check isBmp isn't necessary for correct behavior, but it improves performance in the common case
      // where all characters are in BMP. It short-circuits "enc.canEncode", which is a slow operation. The short
      // circuit is valid because if every char in a string is in BMP, it is definitely encodable as UTF-8.

      // This dance here will replace the original string with one that can be round-tripped.
      return StringUtils.fromUtf8(StringUtils.toUtf8(s));
    } else {
      return s;
    }
  }

  /**
   * Returns whether every character in a string is in BMP (basic multilingual plane).
   */
  private static boolean isBmp(String s)
  {
    for (int i = 0; i < s.length(); i++) {
      final char c = s.charAt(i);

      // All 16-bit code units are either valid code points, or surrogates. So if this char is not a surrogate,
      // it must represent a code point in BMP.
      if (Character.isSurrogate(c)) {
        return false;
      }
    }

    return true;
  }

  private static boolean isFlatList(JsonNode list)
  {
    for (JsonNode obj : list) {
      if (obj.isObject() || obj.isArray()) {
        return false;
      }
    }
    return true;
  }
}
