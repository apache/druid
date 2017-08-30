/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.java.util.common.parsers;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.java.util.common.StringUtils;

import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JSONParser implements Parser<String, Object>
{
  public static final Function<JsonNode, Object> valueFunction = new Function<JsonNode, Object>()
  {
    @Override
    public Object apply(JsonNode node)
    {
      if (node == null || node.isMissingNode() || node.isNull()) {
        return null;
      }
      if (node.isIntegralNumber()) {
        if (node.canConvertToLong()) {
          return node.asLong();
        } else {
          return node.asDouble();
        }
      }
      if (node.isFloatingPointNumber()) {
        return node.asDouble();
      }
      final String s = node.asText();
      final CharsetEncoder enc = Charsets.UTF_8.newEncoder();
      if (s != null && !enc.canEncode(s)) {
        // Some whacky characters are in this string (e.g. \uD900). These are problematic because they are decodeable
        // by new String(...) but will not encode into the same character. This dance here will replace these
        // characters with something more sane.
        return StringUtils.fromUtf8(StringUtils.toUtf8(s));
      } else {
        return s;
      }
    }
  };

  private final ObjectMapper objectMapper;
  private ArrayList<String> fieldNames;
  private final Set<String> exclude;

  public JSONParser()
  {
    this(new ObjectMapper(), null, null);
  }

  @Deprecated
  public JSONParser(Iterable<String> fieldNames)
  {
    this(new ObjectMapper(), fieldNames, null);
  }

  public JSONParser(ObjectMapper objectMapper, Iterable<String> fieldNames)
  {
    this(objectMapper, fieldNames, null);
  }

  public JSONParser(ObjectMapper objectMapper, Iterable<String> fieldNames, Iterable<String> exclude)
  {
    this.objectMapper = objectMapper;
    if (fieldNames != null) {
      setFieldNames(fieldNames);
    }
    this.exclude = exclude != null ? Sets.newHashSet(exclude) : Sets.<String>newHashSet();
  }

  @Override
  public List<String> getFieldNames()
  {
    return fieldNames;
  }

  @Override
  public void setFieldNames(Iterable<String> fieldNames)
  {
    ParserUtils.validateFields(fieldNames);
    this.fieldNames = Lists.newArrayList(fieldNames);
  }

  @Override
  public Map<String, Object> parse(String input)
  {
    try {
      Map<String, Object> map = new LinkedHashMap<>();
      JsonNode root = objectMapper.readTree(input);

      Iterator<String> keysIter = (fieldNames == null ? root.fieldNames() : fieldNames.iterator());

      while (keysIter.hasNext()) {
        String key = keysIter.next();

        if (exclude.contains(key)) {
          continue;
        }

        JsonNode node = root.path(key);

        if (node.isArray()) {
          final List<Object> nodeValue = Lists.newArrayListWithExpectedSize(node.size());
          for (final JsonNode subnode : node) {
            final Object subnodeValue = valueFunction.apply(subnode);
            if (subnodeValue != null) {
              nodeValue.add(subnodeValue);
            }
          }
          map.put(key, nodeValue);
        } else {
          final Object nodeValue = valueFunction.apply(node);
          if (nodeValue != null) {
            map.put(key, nodeValue);
          }
        }
      }
      return map;
    }
    catch (Exception e) {
      throw new ParseException(e, "Unable to parse row [%s]", input);
    }
  }
}
