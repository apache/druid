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

package org.apache.druid.jackson;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import org.apache.druid.java.util.common.IAE;

import java.util.HashMap;
import java.util.Map;

/**
 * A custom {@link com.fasterxml.jackson.databind.jsontype.TypeIdResolver} that enforces strict type id validation.
 * <p>
 * During deserialization, the JSON property used as the type discriminator must match one of the registered subtypes.
 * <p>
 * If the type is missing, the resolver can fall back to a configured default implementation.
 */
public class StrictTypeIdResolver extends TypeIdResolverBase
{
  private Map<String, Class<?>> typeMap;
  private JavaType baseType;

  @Override
  public void init(JavaType baseType)
  {
    this.baseType = baseType;
  }

  private void ensureTypeMap()
  {
    if (typeMap != null) {
      return;
    }

    typeMap = new HashMap<>();
    Class<?> raw = baseType.getRawClass();
    // walk class hierarchy to collect @JsonSubTypes
    while (raw != null) {
      JsonSubTypes subTypes = raw.getAnnotation(JsonSubTypes.class);
      if (subTypes != null) {
        for (JsonSubTypes.Type t : subTypes.value()) {
          typeMap.put(t.name(), t.value());
        }
      }
      raw = raw.getSuperclass();
    }

    if (typeMap.isEmpty()) {
      throw new IllegalStateException("No @JsonSubTypes found for " + baseType.getRawClass());
    }
  }

  @Override
  public String idFromValue(Object value)
  {
    ensureTypeMap();
    Class<?> clazz = value.getClass();
    // find the name from @JsonSubTypes
    for (Map.Entry<String, Class<?>> e : typeMap.entrySet()) {
      if (e.getValue().equals(clazz)) {
        return e.getKey();
      }
    }
    throw new IllegalArgumentException("Unknown class: " + clazz.getName());
  }

  @Override
  public String idFromValueAndType(Object value, Class<?> suggestedType)
  {
    ensureTypeMap();
    return idFromValue(value);
  }

  @Override
  public JavaType typeFromId(DatabindContext context, String id)
  {
    ensureTypeMap();
    if (id == null) {
      // Missing type, caller decides default
      return null;
    }
    Class<?> clazz = typeMap.get(id);
    if (clazz == null) {
      throw new IAE("Unknown type[%s]", id);
    }
    return context.constructType(clazz);
  }

  @Override
  public Id getMechanism()
  {
    return Id.CUSTOM;
  }
}
