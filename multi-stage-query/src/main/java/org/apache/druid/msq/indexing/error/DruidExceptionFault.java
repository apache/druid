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

package org.apache.druid.msq.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.error.DruidException;

import java.util.Map;
import java.util.Objects;

/**
 * Fault that wraps a {@link DruidException}, preserving its fields for serialization across workers and controllers.
 *
 * Fields are stored as Strings rather than enums so that a newer worker can send a fault to an older controller
 * without causing Jackson deserialization failures from unknown enum values.
 */
@JsonTypeName(DruidExceptionFault.CODE)
public class DruidExceptionFault extends BaseMSQFault
{
  public static final String CODE = "DruidException";

  private final String druidErrorCode;
  private final String persona;
  private final String category;
  private final Map<String, String> context;

  @JsonCreator
  public DruidExceptionFault(
      @JsonProperty("druidErrorCode") final String druidErrorCode,
      @JsonProperty("persona") final String persona,
      @JsonProperty("category") final String category,
      @JsonProperty("errorMessage") final String errorMessage,
      @JsonProperty("context") final Map<String, String> context
  )
  {
    super(CODE, errorMessage);
    this.druidErrorCode = druidErrorCode;
    this.persona = persona;
    this.category = category;
    this.context = context == null ? Map.of() : context;
  }

  public static DruidExceptionFault fromDruidException(final DruidException druidException)
  {
    return new DruidExceptionFault(
        druidException.getErrorCode(),
        druidException.getTargetPersona().name(),
        druidException.getCategory().name(),
        druidException.getMessage(),
        druidException.getContext()
    );
  }

  @JsonProperty
  public String getDruidErrorCode()
  {
    return druidErrorCode;
  }

  @JsonProperty
  public String getPersona()
  {
    return persona;
  }

  @JsonProperty
  public String getCategory()
  {
    return category;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, String> getContext()
  {
    return context;
  }

  @Override
  public DruidException toDruidException()
  {
    final DruidException.Persona personaEnum =
        safeValueOf(DruidException.Persona.class, persona, DruidException.Persona.DEVELOPER);
    final DruidException.Category categoryEnum =
        safeValueOf(DruidException.Category.class, category, DruidException.Category.UNCATEGORIZED);
    return DruidException.forPersona(personaEnum)
                         .ofCategory(categoryEnum)
                         .withErrorCode(druidErrorCode)
                         .wasDeserialized()
                         .build(getErrorMessage())
                         .withContext(context);
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
    if (!super.equals(o)) {
      return false;
    }
    final DruidExceptionFault that = (DruidExceptionFault) o;
    return Objects.equals(druidErrorCode, that.druidErrorCode)
           && Objects.equals(persona, that.persona)
           && Objects.equals(category, that.category)
           && Objects.equals(context, that.context);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), druidErrorCode, persona, category, context);
  }

  /**
   * Utility function that returns an enum with a particular name, if it exists, or a default value otherwise.
   * This exists to make it possible to add new {@link DruidException.Category} without breaking serde during
   * rolling updates. Older code attempting to deserialize a new value will get a default value instead.
   */
  private static <T extends Enum<T>> T safeValueOf(final Class<T> enumClass, final String name, final T defaultValue)
  {
    try {
      return Enum.valueOf(enumClass, name);
    }
    catch (IllegalArgumentException e) {
      return defaultValue;
    }
  }
}
