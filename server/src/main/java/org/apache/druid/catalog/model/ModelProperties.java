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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.joda.time.Period;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Definition of a top-level property in a catalog object.
 * Provides a set of typical property definitions. Others can be
 * created case-by-case.
 * <p>
 * Property definitions define the property name, validate the value,
 * and merge updates. Properties have a type: but the type is implicit
 * via the validation, as is needed when the type is actually a map
 * which represents a Java object, or when the value is a list.
 */
public interface ModelProperties
{
  interface PropertyDefn<T>
  {
    /**
     * Name of the property as visible to catalog users. All properties are top-level within
     * the {@code properties} object within a catalog spec.
     */
    String name();

    /**
     * Metadata about properties, such as how they apply to SQL table functions.
     *
     * @see {@link PropertyAttributes} for details.
     */
    Map<String, Object> attributes();

    /**
     * The name of the type of this property to be displayed in error messages.
     */
    String typeName();

    /**
     * Validates that the object given is valid for this property. Provides the JSON
     * mapper in case JSON decoding is required.
     */
    void validate(Object value, ObjectMapper jsonMapper);

    /**
     * Merge a property value with an update. Validation of the update
     * is typically done later, once all the updates are applied. The most
     * typical merge is just: use the new value if provided, else the old
     * value.
     */
    Object merge(Object existing, Object update);

    /**
     * Decodes a JSON-encoded value into a corresponding Java value.
     */
    T decode(Object value, ObjectMapper jsonMapper);

    /**
     * Decodes a SQL-encoded value into a corresponding Java value.
     */
    T decodeSqlValue(Object value, ObjectMapper jsonMapper);
  }

  abstract class BasePropertyDefn<T> implements PropertyDefn<T>
  {
    protected final String name;
    protected final Map<String, Object> attributes;

    public BasePropertyDefn(final String name, Map<String, Object> attributes)
    {
      this.name = name;
      this.attributes = attributes == null ? ImmutableMap.of() : attributes;
    }

    public BasePropertyDefn(final String name)
    {
      this(name, null);
    }

    @Override
    public String name()
    {
      return name;
    }

    @Override
    public Map<String, Object> attributes()
    {
      return attributes;
    }

    @Override
    public String typeName()
    {
      return PropertyAttributes.typeName(this);
    }

    @Override
    public Object merge(Object existing, Object update)
    {
      return update == null ? existing : update;
    }

    @Override
    public T decodeSqlValue(Object value, ObjectMapper jsonMapper)
    {
      return decode(value, jsonMapper);
    }

    @Override
    public String toString()
    {
      return getClass().getSimpleName() + "{"
          + "name: " + name
          + ", attributes: " + attributes()
          + "}";
    }
  }

  abstract class SimplePropertyDefn<T> extends BasePropertyDefn<T>
  {
    public final Class<T> valueClass;

    public SimplePropertyDefn(
        final String name,
        final Class<T> valueClass,
        final Map<String, Object> attribs
    )
    {
      super(
          name,
          PropertyAttributes.merge(
              ImmutableMap.of(
                PropertyAttributes.TYPE_NAME,
                valueClass.getSimpleName()
              ),
              attribs
          )
      );
      this.valueClass = valueClass;
    }

    /**
     * Convert the value from the deserialized JSON format to the type
     * required by this field data type. Also used to decode values from
     * SQL parameters. As a side effect, verifies that the value is of
     * the correct type.
     */
    @Override
    public T decode(Object value, ObjectMapper jsonMapper)
    {
      if (value == null) {
        return null;
      }
      try {
        return jsonMapper.convertValue(value, valueClass);
      }
      catch (Exception e) {
        throw new IAE(
            "Value [%s] is not valid for property [%s], expected %s",
            value,
            name,
            typeName()
        );
      }
    }

    /**
     * Validate that the given value is valid for this property.
     * By default, does a value conversion and discards the value.
     */
    @Override
    public void validate(Object value, ObjectMapper jsonMapper)
    {
      decode(value, jsonMapper);
    }

    protected T decodeJson(Object value, ObjectMapper jsonMapper)
    {
      if (value == null) {
        return null;
      }
      try {
        return jsonMapper.readValue((String) value, valueClass);
      }
      catch (Exception e) {
        throw new IAE(
            "Value [%s] is not valid for property [%s]",
            value,
            name
        );
      }
    }
  }

  class TypeRefPropertyDefn<T> extends BasePropertyDefn<T>
  {
    public final TypeReference<T> valueType;

    public TypeRefPropertyDefn(
        final String name,
        final String typeName,
        final TypeReference<T> valueType,
        final Map<String, Object> attribs
    )
    {
      super(
          name,
          PropertyAttributes.merge(
              ImmutableMap.of(
                PropertyAttributes.TYPE_NAME,
                typeName
              ),
              attribs
          )
      );
      this.valueType = valueType;
    }

    @Override
    public T decode(Object value, ObjectMapper jsonMapper)
    {
      if (value == null) {
        return null;
      }
      try {
        return jsonMapper.convertValue(value, valueType);
      }
      catch (Exception e) {
        throw new IAE(
            "Value [%s] is not valid for property [%s], expected %s",
            value,
            name,
            typeName()
        );
      }
    }

    /**
     * Convert the value from the deserialized JSON format to the type
     * required by this field data type. Also used to decode values from
     * SQL parameters. As a side effect, verifies that the value is of
     * the correct type.
     */
    @Override
    public void validate(Object value, ObjectMapper jsonMapper)
    {
      decode(value, jsonMapper);
    }
  }

  class StringPropertyDefn extends SimplePropertyDefn<String>
  {
    public StringPropertyDefn(String name, Map<String, Object> attribs)
    {
      super(
          name,
          String.class,
          PropertyAttributes.merge(
              ImmutableMap.of(
                  PropertyAttributes.SQL_JAVA_TYPE,
                  String.class
              ),
              attribs
          )
      );
    }
  }

  class GranularityPropertyDefn extends StringPropertyDefn
  {
    public GranularityPropertyDefn(String name, Map<String, Object> attribs)
    {
      super(name, attribs);
    }

    @Override
    public void validate(Object value, ObjectMapper jsonMapper)
    {
      String gran = decode(value, jsonMapper);
      validateGranularity(gran);
    }

    public void validateGranularity(String value)
    {
      if (value == null) {
        return;
      }
      try {
        //noinspection ResultOfObjectAllocationIgnored
        new PeriodGranularity(new Period(value), null, null);
      }
      catch (IllegalArgumentException e) {
        throw new IAE(StringUtils.format("[%s] is an invalid granularity string", value));
      }
    }
  }

  class IntPropertyDefn extends SimplePropertyDefn<Integer>
  {
    public IntPropertyDefn(String name, Map<String, Object> attribs)
    {
      super(
          name,
          Integer.class,
          PropertyAttributes.merge(
              ImmutableMap.of(
                  PropertyAttributes.SQL_JAVA_TYPE,
                  Integer.class
              ),
              attribs
          )
      );
    }
  }

  class BooleanPropertyDefn extends SimplePropertyDefn<Boolean>
  {
    public BooleanPropertyDefn(String name, Map<String, Object> attribs)
    {
      super(
          name,
          Boolean.class,
          PropertyAttributes.merge(
              ImmutableMap.of(
                  PropertyAttributes.SQL_JAVA_TYPE,
                  Boolean.class
              ),
              attribs
          )
      );
    }
  }

  class ListPropertyDefn<T> extends TypeRefPropertyDefn<List<T>>
  {
    public ListPropertyDefn(
        final String name,
        final String typeName,
        final TypeReference<List<T>> valueType,
        final Map<String, Object> attribs
    )
    {
      super(name, typeName, valueType, attribs);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object merge(Object existing, Object updates)
    {
      if (updates == null) {
        return existing;
      }
      if (existing == null) {
        return updates;
      }
      List<T> existingList;
      List<T> updatesList;
      try {
        existingList = (List<T>) existing;
        updatesList = (List<T>) updates;
      }
      catch (ClassCastException e) {
        throw new IAE(
            "Value of field %s must be a list, found %s",
            name,
            updates.getClass().getSimpleName()
        );
      }
      Set<T> existingSet = new HashSet<>(existingList);
      List<T> revised = new ArrayList<>(existingList);
      for (T col : updatesList) {
        if (!existingSet.contains(col)) {
          revised.add(col);
        }
      }
      return revised;
    }
  }

  class StringListPropertyDefn extends ListPropertyDefn<String>
  {
    public StringListPropertyDefn(
        final String name,
        final Map<String, Object> attribs
    )
    {
      super(
          name,
          "string list",
          new TypeReference<List<String>>() {},
          PropertyAttributes.merge(
              ImmutableMap.of(
                  PropertyAttributes.SQL_JAVA_TYPE,
                  String.class
              ),
              attribs
          )
      );
    }

    @Override
    public List<String> decodeSqlValue(Object value, ObjectMapper jsonMapper)
    {
      if (!(value instanceof String)) {
        throw new IAE(StringUtils.format("Argument [%s] is not a VARCHAR", value));
      }
      String[] values = ((String) value).split(",\\s*");
      return Arrays.asList(values);
    }
  }
}
