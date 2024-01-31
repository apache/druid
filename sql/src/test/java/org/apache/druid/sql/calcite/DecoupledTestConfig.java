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

package org.apache.druid.sql.calcite;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface DecoupledTestConfig
{
  NativeQueryIgnore nativeQueryIgnore() default NativeQueryIgnore.NONE;

  enum NativeQueryIgnore
  {
    NONE,
    // decoupled has moved virtualcolumn to postagg (improved plan)
    // CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS
    EXPR_POSTAGG,
    // dim1/dim2 exchange
    AGG_COL_EXCHANGE,
    // this happens when AGGREGATE_REMOVE gets supressed by
    // AGGREGATE_CASE_REWRITE
    AGGREGATE_REMOVE_NOT_FIRED,
    // improved plan - AGGREGATE_ANY_PULL_UP_CONSTANTS ; enable for default?
    IMPROVED_PLAN,
    // worse plan; may loose vectorization; but no extra queries
    SLIGHTLY_WORSE_PLAN;

    public boolean isPresent()
    {
      return this != NONE;
    }
  };

}
