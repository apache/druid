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
  public static final String NULL_HANDLING_LEGACY_LOGICAL_OPS_STRING = "druid.expressions.useLegacyLogicalOperators";

  @JsonProperty("allowNestedArrays")
  private final boolean allowNestedArrays;

  @JsonProperty("useLegacyLogicalOperators")
  private final boolean useLegacyLogicalOperators;

  @JsonCreator
  public ExpressionProcessingConfig(
      @JsonProperty("allowNestedArrays") @Nullable Boolean allowNestedArrays,
      @JsonProperty("useLegacyLogicalOperators") @Nullable Boolean useLegacyLogicalOperators
  )
  {
    this.allowNestedArrays = allowNestedArrays == null
                             ? Boolean.valueOf(System.getProperty(NESTED_ARRAYS_CONFIG_STRING, "false"))
                             : allowNestedArrays;
    if (useLegacyLogicalOperators == null) {
      this.useLegacyLogicalOperators = Boolean.parseBoolean(
          System.getProperty(NULL_HANDLING_LEGACY_LOGICAL_OPS_STRING, "false")
      );
    } else {
      this.useLegacyLogicalOperators = useLegacyLogicalOperators;
    }
  }

  public boolean allowNestedArrays()
  {
    return allowNestedArrays;
  }

  public boolean isUseLegacyLogicalOperators()
  {
    return useLegacyLogicalOperators;
  }
}
