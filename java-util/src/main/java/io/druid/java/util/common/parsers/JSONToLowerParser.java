/*
 * Copyright 2011,2012 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.java.util.common.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
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
public class JSONToLowerParser extends JSONParser
{
  private final ObjectMapper objectMapper;
  private final Set<String> exclude;

  private ArrayList<String> fieldNames;

  public JSONToLowerParser(
      ObjectMapper objectMapper, Iterable<String> fieldNames, Iterable<String> exclude
  )
  {
    super(objectMapper, fieldNames, exclude);
    this.objectMapper = objectMapper;
    if (fieldNames != null) {
      setFieldNames(fieldNames);
    }
    this.exclude = exclude != null ? Sets.newHashSet(
        Iterables.transform(
            exclude,
            new Function<String, String>()
            {
              @Override
              public String apply(String input)
              {
                return input.toLowerCase();
              }
            }
        )
    ) : Sets.<String>newHashSet();
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

        if (exclude.contains(key.toLowerCase())) {
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
          map.put(key.toLowerCase(), nodeValue); // difference from JSONParser parse()
        } else {
          final Object nodeValue = valueFunction.apply(node);
          if (nodeValue != null) {
            map.put(key.toLowerCase(), nodeValue); // difference from JSONParser parse()
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
