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

package org.apache.druid.data.input.impl.systemfield;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.EnumSet;
import java.util.Objects;

/**
 * Container of {@link SystemField}. Wrapper around an {@link EnumSet}.
 */
public class SystemFields
{
  private static final SystemFields NONE = new SystemFields(EnumSet.noneOf(SystemField.class));

  private final EnumSet<SystemField> fields;

  @JsonCreator
  public SystemFields(EnumSet<SystemField> fields)
  {
    this.fields = fields;
  }

  public static SystemFields none()
  {
    return NONE;
  }

  @JsonValue
  public EnumSet<SystemField> getFields()
  {
    return fields;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SystemFields that = (SystemFields) o;
    return Objects.equals(fields, that.fields);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(fields);
  }

  @Override
  public String toString()
  {
    return fields.toString();
  }
}
