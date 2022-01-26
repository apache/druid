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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;

import javax.annotation.Nullable;

/**
 * Like {@link org.apache.druid.common.config.NullHandling}, except for expressions processing configs
 */
public class ExpressionProcessing
{
  /**
   * INSTANCE is injected using static injection to avoid adding JacksonInject annotations all over the code.
   * @see {@link ExpressionProcessingModule} for details.
   *
   * It does not take effect in all unit tests since we don't use Guice Injection. Use {@link #initializeForTests}
   * when modules are not available.
   */
  @Inject
  private static ExpressionProcessingConfig INSTANCE;


  /**
   * Many unit tests do not setup modules for this value to be injected, this method provides a manual way to initialize
   * {@link #INSTANCE}
   * @param allowNestedArrays
   */
  @VisibleForTesting
  public static void initializeForTests(@Nullable Boolean allowNestedArrays)
  {
    INSTANCE = new ExpressionProcessingConfig(allowNestedArrays, null, null);
  }

  @VisibleForTesting
  public static void initializeForStrictBooleansTests(boolean useStrict)
  {
    INSTANCE = new ExpressionProcessingConfig(null, useStrict, null);
  }

  /**
   * [['is expression support for'],['nested arrays'],['enabled?']]
   */
  public static boolean allowNestedArrays()
  {
    // this should only be null in a unit test context
    // in production this will be injected by the expression processing module
    if (INSTANCE == null) {
      throw new IllegalStateException(
          "Expressions module not initialized, call ExpressionProcessing.initializeForTests()"
      );
    }
    return INSTANCE.allowNestedArrays();
  }


  public static boolean useStrictBooleans()
  {
    // this should only be null in a unit test context, in production this will be injected by the null handling module
    if (INSTANCE == null) {
      throw new IllegalStateException("ExpressionProcessing module not initialized, call ExpressionProcessing.initializeForTests()");
    }
    return INSTANCE.isUseStrictBooleans();
  }


  public static boolean processArraysAsMultiValueStrings()
  {
    // this should only be null in a unit test context, in production this will be injected by the null handling module
    if (INSTANCE == null) {
      throw new IllegalStateException(
          "ExpressionProcessing module not initialized, call ExpressionProcessing.initializeForTests()"
      );
    }
    return INSTANCE.processArraysAsMultiValueStrings();
  }
}
