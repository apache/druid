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

package org.apache.druid.iceberg.input;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Controls how residual filters are handled during Iceberg table scanning.
 *
 * When an Iceberg filter is applied on a non-partition column, the filtering happens at the
 * file metadata level only. Files that might contain matching rows are returned, but these
 * files may include "residual" rows that don't actually match the filter. These residual rows
 * would need to be filtered on the Druid side using a filter in transformSpec.
 */
public enum ResidualFilterMode
{
  /**
   * Ignore residual filters. This is the default behavior for backward compatibility.
   * Residual rows will be ingested unless filtered by transformSpec.
   */
  IGNORE("ignore"),

  /**
   * Log a warning when residual filters are detected, but continue with ingestion.
   * Useful for identifying potential data quality issues without failing the job.
   */
  WARN("warn"),

  /**
   * Fail the ingestion job when residual filters are detected.
   * Use this mode to ensure that only partition-column filters are used,
   * preventing unintended residual rows from being ingested.
   */
  FAIL("fail");

  private final String value;

  ResidualFilterMode(String value)
  {
    this.value = value;
  }

  @JsonValue
  public String getValue()
  {
    return value;
  }

  public static ResidualFilterMode fromString(String value)
  {
    for (ResidualFilterMode mode : values()) {
      if (mode.value.equalsIgnoreCase(value)) {
        return mode;
      }
    }
    throw new IllegalArgumentException(
        "Unknown residualFilterMode: " + value + ". Valid values are: ignore, warn, fail"
    );
  }
}
