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

import javax.annotation.Nullable;

public class ExpressionProcessingConfig
{
  public static final String NESTED_ARRAYS_CONFIG_STRING = "druid.expressions.allowNestedArrays";
  public static final String NULL_HANDLING_LEGACY_LOGICAL_OPS_STRING = "druid.expressions.useStrictBooleans";
  // Coerce arrays to multi value strings
  public static final String PROCESS_ARRAYS_AS_MULTIVALUE_STRINGS_CONFIG_STRING =
      "druid.expressions.processArraysAsMultiValueStrings";
  // Coerce 'null', '[]', and '[null]' into '[null]' for backwards compat with 0.22 and earlier
  public static final String HOMOGENIZE_NULL_MULTIVALUE_STRING_ARRAYS =
      "druid.expressions.homogenizeNullMultiValueStringArrays";

  @JsonProperty("allowNestedArrays")
  private final boolean allowNestedArrays;

  @JsonProperty("useStrictBooleans")
  private final boolean useStrictBooleans;

  @JsonProperty("processArraysAsMultiValueStrings")
  private final boolean processArraysAsMultiValueStrings;

  @JsonProperty("homogenizeNullMultiValueStringArrays")
  private final boolean homogenizeNullMultiValueStringArrays;

  @JsonCreator
  public ExpressionProcessingConfig(
      @JsonProperty("allowNestedArrays") @Nullable Boolean allowNestedArrays,
      @JsonProperty("useStrictBooleans") @Nullable Boolean useStrictBooleans,
      @JsonProperty("processArraysAsMultiValueStrings") @Nullable Boolean processArraysAsMultiValueStrings,
      @JsonProperty("homogenizeNullMultiValueStringArrays") @Nullable Boolean homogenizeNullMultiValueStringArrays
  )
  {
    this.allowNestedArrays = getWithPropertyFallbackFalse(allowNestedArrays, NESTED_ARRAYS_CONFIG_STRING);
    this.useStrictBooleans = getWithPropertyFallbackFalse(useStrictBooleans, NULL_HANDLING_LEGACY_LOGICAL_OPS_STRING);
    this.processArraysAsMultiValueStrings = getWithPropertyFallbackFalse(
        processArraysAsMultiValueStrings,
        PROCESS_ARRAYS_AS_MULTIVALUE_STRINGS_CONFIG_STRING
    );
    this.homogenizeNullMultiValueStringArrays = getWithPropertyFallbackFalse(
        homogenizeNullMultiValueStringArrays,
        HOMOGENIZE_NULL_MULTIVALUE_STRING_ARRAYS
    );
  }

  public boolean allowNestedArrays()
  {
    return allowNestedArrays;
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
    return value != null ? value : Boolean.valueOf(System.getProperty(property, "false"));
  }
}
