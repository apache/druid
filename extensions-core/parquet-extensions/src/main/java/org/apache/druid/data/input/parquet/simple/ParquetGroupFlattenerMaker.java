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

package org.apache.druid.data.input.parquet.simple;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JsonProvider;
import org.apache.druid.java.util.common.parsers.NotImplementedMappingProvider;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.Type;

import javax.annotation.Nullable;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ParquetGroupFlattenerMaker implements ObjectFlatteners.FlattenerMaker<Group>
{
  private final Configuration jsonPathConfiguration;
  private final ParquetGroupConverter converter;
  private final JsonProvider parquetJsonProvider;

  public ParquetGroupFlattenerMaker(boolean binaryAsString)
  {
    this.converter = new ParquetGroupConverter(binaryAsString);
    this.parquetJsonProvider = new ParquetGroupJsonProvider(converter);
    this.jsonPathConfiguration = Configuration.builder()
                                              .jsonProvider(parquetJsonProvider)
                                              .mappingProvider(new NotImplementedMappingProvider())
                                              .options(EnumSet.of(Option.SUPPRESS_EXCEPTIONS))
                                              .build();
  }

  @Override
  public Set<String> discoverRootFields(Group obj)
  {
    return obj.getType()
              .getFields()
              .stream()
              .filter(Type::isPrimitive)
              .map(Type::getName)
              .collect(Collectors.toSet());
  }

  @Override
  public Object getRootField(Group obj, String key)
  {
    Object val = converter.convertField(obj, key);
    return finalizeConversion(val);
  }

  @Override
  public Function<Group, Object> makeJsonPathExtractor(String expr)
  {
    final JsonPath jsonPath = JsonPath.compile(expr);
    return record -> {
      Object val = jsonPath.read(record, jsonPathConfiguration);
      return finalizeConversion(val);
    };
  }

  @Nullable
  @Override
  public Function<Group, Object> makeJsonQueryExtractor(String expr)
  {
    throw new UnsupportedOperationException("Parquet does not support JQ");
  }

  @Override
  public JsonProvider getJsonProvider()
  {
    return parquetJsonProvider;
  }

  @Override
  public Object finalizeConversionForMap(Object o)
  {
    return finalizeConversion(o);
  }

  /**
   * After json conversion, wrapped list items can still need unwrapped. See
   * {@link ParquetGroupConverter#isWrappedListPrimitive(Object)} and
   * {@link ParquetGroupConverter#unwrapListPrimitive(Object)} for more details.
   *
   * @param o
   *
   * @return
   */
  private Object finalizeConversion(Object o)
  {
    // conversion can leave 'wrapped' list primitives
    if (ParquetGroupConverter.isWrappedListPrimitive(o)) {
      return converter.unwrapListPrimitive(o);
    } else if (o instanceof List) {
      List<Object> asList = ((List<?>) o).stream().filter(Objects::nonNull).collect(Collectors.toList());
      if (asList.stream().allMatch(ParquetGroupConverter::isWrappedListPrimitive)) {
        return asList.stream().map(Group.class::cast).map(converter::unwrapListPrimitive).collect(Collectors.toList());
      }
    }
    return o;
  }
}
