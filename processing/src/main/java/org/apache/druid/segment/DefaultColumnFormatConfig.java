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

package org.apache.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.error.DruidException;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;

public class DefaultColumnFormatConfig
{
  public static void validateNestedFormatVersion(@Nullable Integer formatVersion)
  {
    if (formatVersion != null) {
      if (formatVersion < 4 || formatVersion > 5) {
        throw DruidException.forPersona(DruidException.Persona.USER)
                            .ofCategory(DruidException.Category.INVALID_INPUT)
                            .build("Unsupported nested column format version[%s]", formatVersion);
      }
    }
  }

  private static void validateMultiValueHandlingMode(@Nullable String multiValueHandlingMode)
  {
    if (multiValueHandlingMode != null) {
      try {
        DimensionSchema.MultiValueHandling.fromString(multiValueHandlingMode);
      }
      catch (IllegalArgumentException e) {
        throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                            .ofCategory(DruidException.Category.INVALID_INPUT)
                            .build(
                                "Invalid value[%s] specified for 'druid.indexing.formats.multiValueHandlingMode'."
                                + " Supported values are [%s].",
                                multiValueHandlingMode,
                                Arrays.toString(DimensionSchema.MultiValueHandling.values())
                            );
      }
    }
  }

  @Nullable
  @JsonProperty("nestedColumnFormatVersion")
  private final Integer nestedColumnFormatVersion;

  @JsonProperty("multiValueHandlingMode")
  private final DimensionSchema.MultiValueHandling multiValueHandlingMode;

  @JsonCreator
  public DefaultColumnFormatConfig(
      @JsonProperty("nestedColumnFormatVersion") @Nullable Integer nestedColumnFormatVersion,
      @JsonProperty("multiValueHandlingMode") @Nullable String multiValueHandlingMode
  )
  {
    validateNestedFormatVersion(nestedColumnFormatVersion);
    validateMultiValueHandlingMode(multiValueHandlingMode);

    this.nestedColumnFormatVersion = nestedColumnFormatVersion;
    this.multiValueHandlingMode = multiValueHandlingMode == null ?
                                  DimensionSchema.MultiValueHandling.SORTED_ARRAY :
                                  DimensionSchema.MultiValueHandling.fromString(multiValueHandlingMode);
  }

  @Nullable
  @JsonProperty("nestedColumnFormatVersion")
  public Integer getNestedColumnFormatVersion()
  {
    return nestedColumnFormatVersion;
  }

  @JsonProperty("multiValueHandlingMode")
  public DimensionSchema.MultiValueHandling getMultiValueHandlingMode()
  {
    return multiValueHandlingMode;
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
    DefaultColumnFormatConfig that = (DefaultColumnFormatConfig) o;
    return Objects.equals(nestedColumnFormatVersion, that.nestedColumnFormatVersion)
           && multiValueHandlingMode == that.multiValueHandlingMode;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(nestedColumnFormatVersion, multiValueHandlingMode);
  }

  @Override
  public String toString()
  {
    return "DefaultColumnFormatConfig{" +
           "nestedColumnFormatVersion=" + nestedColumnFormatVersion +
           ", multiValueHandlingMode=" + multiValueHandlingMode +
           '}';
  }
}
