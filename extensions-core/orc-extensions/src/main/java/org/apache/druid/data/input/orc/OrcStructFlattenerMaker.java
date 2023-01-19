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

package org.apache.druid.data.input.orc;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JsonProvider;
import org.apache.druid.java.util.common.parsers.NotImplementedMappingProvider;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcMap;
import org.apache.orc.mapred.OrcStruct;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Function;

public class OrcStructFlattenerMaker implements ObjectFlatteners.FlattenerMaker<OrcStruct>
{
  private final Configuration jsonPathConfiguration;
  private final JsonProvider orcJsonProvider;
  private final OrcStructConverter converter;

  private final boolean discoverNestedFields;

  OrcStructFlattenerMaker(boolean binaryAsString, boolean disocverNestedFields)
  {
    this.converter = new OrcStructConverter(binaryAsString);
    this.orcJsonProvider = new OrcStructJsonProvider(converter);
    this.jsonPathConfiguration = Configuration.builder()
                                              .jsonProvider(orcJsonProvider)
                                              .mappingProvider(new NotImplementedMappingProvider())
                                              .options(EnumSet.of(Option.SUPPRESS_EXCEPTIONS))
                                              .build();
    this.discoverNestedFields = disocverNestedFields;
  }

  @Override
  public Iterable<String> discoverRootFields(OrcStruct obj)
  {
    // if discovering nested fields, just return all root fields since we want everything
    // else, we filter for literals and arrays of literals
    if (discoverNestedFields) {
      return obj.getSchema().getFieldNames();
    }
    List<String> fields = obj.getSchema().getFieldNames();
    List<TypeDescription> children = obj.getSchema().getChildren();
    List<String> primitiveFields = new ArrayList<>();
    for (int i = 0; i < fields.size(); i++) {
      if (children.get(i).getCategory().isPrimitive() || (children.get(i).getCategory().equals(TypeDescription.Category.LIST) &&
                                                          children.get(i).getChildren().get(0).getCategory().isPrimitive())) {
        primitiveFields.add(fields.get(i));
      }
    }
    return primitiveFields;
  }

  @Override
  public Object getRootField(OrcStruct obj, String key)
  {
    return toPlainJavaType(converter.convertRootField(obj, key));
  }

  @Override
  public Function<OrcStruct, Object> makeJsonPathExtractor(String expr)
  {
    final JsonPath jsonPath = JsonPath.compile(expr);
    return record -> {
      Object val = jsonPath.read(record, jsonPathConfiguration);
      return finalizeConversion(val);
    };
  }

  @Nullable
  @Override
  public Function<OrcStruct, Object> makeJsonQueryExtractor(String expr)
  {
    throw new UnsupportedOperationException("ORC flattener does not support JQ");
  }

  @Override
  public Function<OrcStruct, Object> makeJsonTreeExtractor(List<String> nodes)
  {
    if (nodes.size() == 1) {
      return (OrcStruct record) -> getRootField(record, nodes.get(0));
    }

    throw new UnsupportedOperationException("ORC flattener does not support nested root queries");
  }

  @Override
  public JsonProvider getJsonProvider()
  {
    return orcJsonProvider;
  }

  @Override
  public Object finalizeConversionForMap(Object o)
  {
    return finalizeConversion(o);
  }

  private Object finalizeConversion(Object o)
  {
    // recursively convert any complex types
    if (o instanceof OrcStruct || o instanceof OrcMap || o instanceof OrcList) {
      return toPlainJavaType(o);
    } else if (o instanceof WritableComparable) {
      return converter.tryConvertPrimitive((WritableComparable) o);
    }
    return o;
  }
}
