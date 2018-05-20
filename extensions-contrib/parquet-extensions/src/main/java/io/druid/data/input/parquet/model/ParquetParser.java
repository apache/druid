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
package io.druid.data.input.parquet.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ParquetParser
{

  public static final int DEFAULT_MAX_DEPTH = 3;
  private final transient Map<Class<?>, FieldType> typeMap;
  private final List<Field> fields;
  private final Integer maxDepth;

  @JsonCreator
  public ParquetParser(@JsonProperty("fields") List<Field> fields, @JsonProperty("depth") int maxDepth)
  {
    this.fields = fields;
    this.maxDepth = Math.max(maxDepth, DEFAULT_MAX_DEPTH);
    //Loads all the mapping between Class reference and FieldType
    final Map<Class<?>, FieldType> tempMap = Maps.newHashMap();
    for (FieldType fieldType : FieldType.values()) {
      tempMap.put(fieldType.getClassz(), fieldType);
    }
    typeMap = Collections.unmodifiableMap(tempMap);
  }

  /**
   * For given class returns the appropriate available field type
   *
   * @param classz - class type of incoming object reference
   *
   * @return FieldType enum
   */
  public FieldType getFieldType(Class<?> classz)
  {
    return typeMap.get(classz);
  }

  public List<Field> getFields()
  {
    return fields;
  }

  public boolean containsType(String typeName)
  {
    for (Field field : this.getFields()) {
      if (field.getKey().toString().equals(typeName) || (field.getRootFieldName() != null &&
                                                         field.getRootFieldName().equals(typeName))) {
        return true;
      }
    }
    return false;
  }

  public Integer getMaxDepth()
  {
    return maxDepth;
  }
}
