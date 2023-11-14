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

package org.apache.druid.math.expr;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;

public class ExpressionProcessingConfig
{
  private static final Logger LOG = new Logger(ExpressionProcessingConfig.class);

  public static final String NULL_HANDLING_LEGACY_LOGICAL_OPS_STRING = "druid.expressions.useStrictBooleans";
  // Coerce arrays to multi value strings
  public static final String PROCESS_ARRAYS_AS_MULTIVALUE_STRINGS_CONFIG_STRING =
      "druid.expressions.processArraysAsMultiValueStrings";
  // Coerce 'null', '[]', and '[null]' into '[null]' for backwards compat with 0.22 and earlier
  public static final String HOMOGENIZE_NULL_MULTIVALUE_STRING_ARRAYS =
      "druid.expressions.homogenizeNullMultiValueStringArrays";

  @JsonProperty("useStrictBooleans")
  private final boolean useStrictBooleans;

  @JsonProperty("processArraysAsMultiValueStrings")
  private final boolean processArraysAsMultiValueStrings;

  @JsonProperty("homogenizeNullMultiValueStringArrays")
  private final boolean homogenizeNullMultiValueStringArrays;

  @JsonCreator
  public ExpressionProcessingConfig(
      @JsonProperty("useStrictBooleans") @Nullable Boolean useStrictBooleans,
      @JsonProperty("processArraysAsMultiValueStrings") @Nullable Boolean processArraysAsMultiValueStrings,
      @JsonProperty("homogenizeNullMultiValueStringArrays") @Nullable Boolean homogenizeNullMultiValueStringArrays
  )
  {
    this.useStrictBooleans = getWithPropertyFallback(
        useStrictBooleans,
        NULL_HANDLING_LEGACY_LOGICAL_OPS_STRING,
        "true"
    );
    this.processArraysAsMultiValueStrings = getWithPropertyFallbackFalse(
        processArraysAsMultiValueStrings,
        PROCESS_ARRAYS_AS_MULTIVALUE_STRINGS_CONFIG_STRING
    );
    this.homogenizeNullMultiValueStringArrays = getWithPropertyFallbackFalse(
        homogenizeNullMultiValueStringArrays,
        HOMOGENIZE_NULL_MULTIVALUE_STRING_ARRAYS
    );
    String version = ExpressionProcessingConfig.class.getPackage().getImplementationVersion();
    if (version == null || version.contains("SNAPSHOT")) {
      version = "latest";
    }
    final String docsBaseFormat = "https://druid.apache.org/docs/%s/querying/sql-data-types#%s";
    if (!this.useStrictBooleans) {
      LOG.warn(
          "druid.expressions.useStrictBooleans set to 'false', we recommend using 'true' if using SQL to query Druid for the most SQL compliant behavior, see %s for details",
          StringUtils.format(docsBaseFormat, version, "boolean-logic")
      );
    }
  }

  public boolean isUseStrictBooleans()
  {
    return useStrictBooleans;
  }

  public boolean processArraysAsMultiValueStrings()
  {
    return processArraysAsMultiValueStrings;
  }

  public boolean isHomogenizeNullMultiValueStringArrays()
  {
    return homogenizeNullMultiValueStringArrays;
  }

  private static boolean getWithPropertyFallbackFalse(@Nullable Boolean value, String property)
  {
    return getWithPropertyFallback(value, property, "false");
  }

  private static boolean getWithPropertyFallback(@Nullable Boolean value, String property, String fallback)
  {
    return value != null ? value : Boolean.valueOf(System.getProperty(property, fallback));
  }
}
