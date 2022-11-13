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

package org.apache.druid.catalog.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.model.ModelProperties.PropertyDefn;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Metadata definition of the metadata objects stored in the catalog. (Yes,
 * that means that this is meta-meta-data.) Objects consist of a map of
 * property values (and perhaps other items defined in subclasses.) Each
 * property is defined by a column metadata object. Objects allow extended
 * properties which have no definition: the meaning of such properties is
 * defined elsewhere.
 */
public class ObjectDefn
{
  private final String name;
  private final String typeValue;
  private final Map<String, PropertyDefn<?>> properties;

  public ObjectDefn(
      final String name,
      final String typeValue,
      final List<PropertyDefn<?>> fields
  )
  {
    this.name = name;
    this.typeValue = typeValue;
    this.properties = toPropertyMap(fields);
  }

  protected static Map<String, PropertyDefn<?>> toPropertyMap(final List<PropertyDefn<?>> props)
  {
    ImmutableMap.Builder<String, PropertyDefn<?>> builder = ImmutableMap.builder();
    if (props != null) {
      for (PropertyDefn<?> prop : props) {
        builder.put(prop.name(), prop);
      }
    }
    return builder.build();
  }

  public String name()
  {
    return name;
  }

  /**
   * The type value is the value of the {@code "type"} field written into the
   * object's Java or JSON representation. It is akin to the type used by
   * Jackson.
   */
  public String typeValue()
  {
    return typeValue;
  }

  public Map<String, PropertyDefn<?>> properties()
  {
    return properties;
  }

  public PropertyDefn<?> property(String key)
  {
    return properties.get(key);
  }

  /**
   * Merge the properties for an object using a set of updates in a map. If the
   * update value is {@code null}, then remove the property in the revised set. If the
   * property is known, use the column definition to merge the values. Else, the
   * update replaces any existing value.
   * <p>
   * This method does not validate the properties, except as needed to do a
   * merge. A separate validation step is done on the final, merged object.
   */
  public Map<String, Object> mergeProperties(
      final Map<String, Object> source,
      final Map<String, Object> update
  )
  {
    if (update == null) {
      return source;
    }
    if (source == null) {
      return update;
    }
    Map<String, Object> merged = new HashMap<>(source);
    for (Map.Entry<String, Object> entry : update.entrySet()) {
      if (entry.getValue() == null) {
        merged.remove(entry.getKey());
      } else {
        PropertyDefn<?> propDefn = property(entry.getKey());
        Object value = entry.getValue();
        if (propDefn != null) {
          value = propDefn.merge(merged.get(entry.getKey()), entry.getValue());
        }
        merged.put(entry.getKey(), value);
      }
    }
    return merged;
  }

  /**
   * Validate the property values using the property definitions defined in
   * this class. The list may contain "custom" properties which are accepted
   * as-is.
   */
  public void validate(Map<String, Object> spec, ObjectMapper jsonMapper)
  {
    for (PropertyDefn<?> propDefn : properties.values()) {
      propDefn.validate(spec.get(propDefn.name()), jsonMapper);
    }
  }
}
