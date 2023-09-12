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
import org.apache.druid.error.DruidException;

import javax.annotation.Nullable;
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

  @JsonProperty("nestedColumnFormatVersion")
  private final Integer nestedColumnFormatVersion;

  @JsonCreator
  public DefaultColumnFormatConfig(
      @JsonProperty("nestedColumnFormatVersion") @Nullable Integer nestedColumnFormatVersion
  )
  {
    this.nestedColumnFormatVersion = nestedColumnFormatVersion;
    validateNestedFormatVersion(this.nestedColumnFormatVersion);
  }

  @Nullable
  @JsonProperty("nestedColumnFormatVersion")
  public Integer getNestedColumnFormatVersion()
  {
    return nestedColumnFormatVersion;
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
    return Objects.equals(nestedColumnFormatVersion, that.nestedColumnFormatVersion);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(nestedColumnFormatVersion);
  }

  @Override
  public String toString()
  {
    return "DefaultColumnFormatConfig{" +
           "nestedColumnFormatVersion=" + nestedColumnFormatVersion +
           '}';
  }
}
