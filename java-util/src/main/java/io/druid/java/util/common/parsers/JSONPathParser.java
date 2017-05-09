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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.StringUtils;

import java.math.BigInteger;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JSON parser class that uses the JsonPath library to access fields via path expressions.
 */
public class JSONPathParser implements Parser<String, Object>
{
  private final Map<String, Pair<FieldType, JsonPath>> fieldPathMap;
  private final boolean useFieldDiscovery;
  private final ObjectMapper mapper;
  private final CharsetEncoder enc = Charsets.UTF_8.newEncoder();
  private final Configuration jsonPathConfig;

  /**
   * Constructor
   *
   * @param fieldSpecs        List of field specifications.
   * @param useFieldDiscovery If true, automatically add root fields seen in the JSON document to the parsed object Map.
   *                          Only fields that contain a singular value or flat list (list containing no subobjects or lists) are automatically added.
   * @param mapper            Optionally provide an ObjectMapper, used by the parser for reading the input JSON.
   */
  public JSONPathParser(List<FieldSpec> fieldSpecs, boolean useFieldDiscovery, ObjectMapper mapper)
  {
    this.fieldPathMap = generateFieldPaths(fieldSpecs);
    this.useFieldDiscovery = useFieldDiscovery;
    this.mapper = mapper == null ? new ObjectMapper() : mapper;

    // Avoid using defaultConfiguration, as this depends on json-smart which we are excluding.
    this.jsonPathConfig = Configuration.builder()
                                       .jsonProvider(new JacksonJsonProvider())
                                       .mappingProvider(new JacksonMappingProvider())
                                       .options(EnumSet.of(Option.SUPPRESS_EXCEPTIONS))
                                       .build();
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
  public Map<String, Object> parse(String input)
  {
    try {
      Map<String, Object> map = new LinkedHashMap<>();
      Map<String, Object> document = mapper.readValue(
          input,
          new TypeReference<Map<String, Object>>()
          {
          }
      );
      for (Map.Entry<String, Pair<FieldType, JsonPath>> entry : fieldPathMap.entrySet()) {
        String fieldName = entry.getKey();
        Pair<FieldType, JsonPath> pair = entry.getValue();
        JsonPath path = pair.rhs;
        Object parsedVal;
        if (pair.lhs == FieldType.ROOT) {
          parsedVal = document.get(fieldName);
        } else {
          parsedVal = path.read(document, jsonPathConfig);
        }
        if (parsedVal == null) {
          continue;
        }
        parsedVal = valueConversionFunction(parsedVal);
        map.put(fieldName, parsedVal);
      }
      if (useFieldDiscovery) {
        discoverFields(map, document);
      }
      return map;
    }
    catch (Exception e) {
      throw new ParseException(e, "Unable to parse row [%s]", input);
    }
  }

  private Map<String, Pair<FieldType, JsonPath>> generateFieldPaths(List<FieldSpec> fieldSpecs)
  {
    Map<String, Pair<FieldType, JsonPath>> map = new LinkedHashMap<>();
    for (FieldSpec fieldSpec : fieldSpecs) {
      String fieldName = fieldSpec.getName();
      if (map.get(fieldName) != null) {
        throw new IllegalArgumentException("Cannot have duplicate field definition: " + fieldName);
      }
      JsonPath path = fieldSpec.getType() == FieldType.PATH ? JsonPath.compile(fieldSpec.getExpr()) : null;
      Pair<FieldType, JsonPath> pair = new Pair<>(fieldSpec.getType(), path);
      map.put(fieldName, pair);
    }
    return map;
  }

  private void discoverFields(Map<String, Object> map, Map<String, Object> document)
  {
    for (Map.Entry<String, Object> e : document.entrySet()) {
      String field = e.getKey();
      if (!map.containsKey(field)) {
        Object val = e.getValue();
        if (val == null) {
          continue;
        }
        if (val instanceof Map) {
          continue;
        }
        if (val instanceof List) {
          if (!isFlatList((List) val)) {
            continue;
          }
        }
        val = valueConversionFunction(val);
        map.put(field, val);
      }
    }
  }

  private Object valueConversionFunction(Object val)
  {
    if (val instanceof Integer) {
      return Long.valueOf((Integer) val);
    }

    if (val instanceof BigInteger) {
      return Double.valueOf(((BigInteger) val).doubleValue());
    }

    if (val instanceof String) {
      return charsetFix((String) val);
    }

    if (val instanceof List) {
      List<Object> newList = new ArrayList<>();
      for (Object entry : ((List) val)) {
        newList.add(valueConversionFunction(entry));
      }
      return newList;
    }

    if (val instanceof Map) {
      Map<String, Object> newMap = new LinkedHashMap<>();
      Map<String, Object> valMap = (Map<String, Object>) val;
      for (Map.Entry<String, Object> entry : valMap.entrySet()) {
        newMap.put(entry.getKey(), valueConversionFunction(entry.getValue()));
      }
      return newMap;
    }

    return val;
  }

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

  private boolean isFlatList(List<Object> list)
  {
    for (Object obj : list) {
      if ((obj instanceof Map) || (obj instanceof List)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Specifies access behavior for a field.
   */
  public enum FieldType
  {
    /**
     * A ROOT field is read directly from the JSON document root without using the JsonPath library.
     */
    ROOT,

    /**
     * A PATH field uses a JsonPath expression to retrieve the field value
     */
    PATH;
  }

  /**
   * Specifies a field to be added to the parsed object Map, using JsonPath notation.
   * <p>
   * See <a href="https://github.com/jayway/JsonPath">https://github.com/jayway/JsonPath</a> for more information.
   */
  public static class FieldSpec
  {
    private final FieldType type;
    private final String name;
    private final String expr;

    /**
     * Constructor
     *
     * @param type Specifies how this field should be retrieved.
     * @param name Name of the field, used as the key in the Object map returned by the parser.
     *             For ROOT fields, this must match the field name as it appears in the JSON document.
     * @param expr Only used by PATH type fields, specifies the JsonPath expression used to access the field.
     */
    public FieldSpec(
        FieldType type,
        String name,
        String expr
    )
    {
      this.type = type;
      this.name = name;
      this.expr = expr;
    }

    public FieldType getType()
    {
      return type;
    }

    public String getName()
    {
      return name;
    }

    public String getExpr()
    {
      return expr;
    }
  }
}
