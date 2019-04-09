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
import org.apache.druid.java.util.common.parsers.NotImplementedMappingProvider;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
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
  private final OrcStructConverter converter;

  OrcStructFlattenerMaker(boolean binaryAsString)
  {
    this.converter = new OrcStructConverter(binaryAsString);
    this.jsonPathConfiguration = Configuration.builder()
                                              .jsonProvider(new OrcStructJsonProvider(converter))
                                              .mappingProvider(new NotImplementedMappingProvider())
                                              .options(EnumSet.of(Option.SUPPRESS_EXCEPTIONS))
                                              .build();
  }

  @Override
  public Iterable<String> discoverRootFields(OrcStruct obj)
  {
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
    return finalizeConversion(converter.convertRootField(obj, key));
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

  private Object finalizeConversion(Object o)
  {
    // replace any remaining complex types with null
    if (o instanceof OrcStruct || o instanceof OrcMap || o instanceof OrcList) {
      return null;
    }
    return o;
  }
}
