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
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.StringUtils;

import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * TODO:
 * There is a lot of code copy and pasted from JSONParser. JSONParser needs to be rewritten
 * to actually take a map transformer instead of what it is doing now. For the purposes of moving forward in 0.7.0,
 * I am going to have a different parser to lower case data from JSON. This code needs to be removed the next time
 * we touch java-util.
 */
@Deprecated
public class JSONToLowerParser implements Parser<String, Object>
{
  private static final Function<JsonNode, Object> VALUE_FUNCTION = new Function<JsonNode, Object>()
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
      final CharsetEncoder enc = StandardCharsets.UTF_8.newEncoder();
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
  private final Set<String> exclude;

  private ArrayList<String> fieldNames;

  public JSONToLowerParser(ObjectMapper objectMapper, Iterable<String> fieldNames, Iterable<String> exclude)
  {
    this.objectMapper = objectMapper;
    if (fieldNames != null) {
      setFieldNames(fieldNames);
    }
    this.exclude = exclude != null
                   ? Sets.newHashSet(Iterables.transform(exclude, StringUtils::toLowerCase))
                   : new HashSet<>();
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
  public Map<String, Object> parseToMap(String input)
  {
    try {
      Map<String, Object> map = new LinkedHashMap<>();
      JsonNode root = objectMapper.readTree(input);

      Iterator<String> keysIter = (fieldNames == null ? root.fieldNames() : fieldNames.iterator());

      while (keysIter.hasNext()) {
        String key = keysIter.next();

        if (exclude.contains(StringUtils.toLowerCase(key))) {
          continue;
        }

        JsonNode node = root.path(key);

        if (node.isArray()) {
          final List<Object> nodeValue = Lists.newArrayListWithExpectedSize(node.size());
          for (final JsonNode subnode : node) {
            final Object subnodeValue = VALUE_FUNCTION.apply(subnode);
            if (subnodeValue != null) {
              nodeValue.add(subnodeValue);
            }
          }
          map.put(StringUtils.toLowerCase(key), nodeValue); // difference from JSONParser parse()
        } else {
          final Object nodeValue = VALUE_FUNCTION.apply(node);
          if (nodeValue != null) {
            map.put(StringUtils.toLowerCase(key), nodeValue); // difference from JSONParser parse()
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
