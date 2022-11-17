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
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.joda.time.Period;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
    String name();
    String typeName();
    void validate(Object value, ObjectMapper jsonMapper);

    /**
     * Merge a property value with an update. Validation of the update
     * is typically done later, once all the updates are applied. The most
     * typical merge is just: use the new value if provided, else the old
     * value.
     */
    Object merge(Object existing, Object update);
    T decode(Object value, ObjectMapper jsonMapper);
  }

  abstract class BasePropertyDefn<T> implements PropertyDefn<T>
  {
    protected final String name;

    public BasePropertyDefn(final String name)
    {
      this.name = name;
    }

    @Override
    public String name()
    {
      return name;
    }

    @Override
    public Object merge(Object existing, Object update)
    {
      return update == null ? existing : update;
    }

    @Override
    public String toString()
    {
      return getClass().getSimpleName() + "{"
          + "name: " + name
          + ", type: " + typeName()
          + "}";
    }
  }

  class SimplePropertyDefn<T> extends BasePropertyDefn<T>
  {
    public final Class<T> valueClass;

    public SimplePropertyDefn(
        final String name,
        final Class<T> valueClass
    )
    {
      super(name);
      this.valueClass = valueClass;
    }

    @Override
    public String typeName()
    {
      return valueClass.getSimpleName();
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
  }

  class TypeRefPropertyDefn<T> extends BasePropertyDefn<T>
  {
    public final String typeName;
    public final TypeReference<T> valueType;

    public TypeRefPropertyDefn(
        final String name,
        final String typeName,
        final TypeReference<T> valueType
    )
    {
      super(name);
      this.typeName = Preconditions.checkNotNull(typeName);
      this.valueType = valueType;
    }

    @Override
    public String typeName()
    {
      return typeName;
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
    public StringPropertyDefn(String name)
    {
      super(name, String.class);
    }
  }

  class GranularityPropertyDefn extends StringPropertyDefn
  {
    public GranularityPropertyDefn(String name)
    {
      super(name);
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
    public IntPropertyDefn(String name)
    {
      super(name, Integer.class);
    }
  }

  class BooleanPropertyDefn extends SimplePropertyDefn<Boolean>
  {
    public BooleanPropertyDefn(String name)
    {
      super(name, Boolean.class);
    }
  }

  class ListPropertyDefn<T> extends TypeRefPropertyDefn<List<T>>
  {
    public ListPropertyDefn(
        final String name,
        final String typeName,
        final TypeReference<List<T>> valueType
    )
    {
      super(name, typeName, valueType);
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
    public StringListPropertyDefn(String name)
    {
      super(
          name,
          "string list",
          new TypeReference<List<String>>() {}
      );
    }
  }
}
