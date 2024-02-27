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

package org.apache.druid.sql.calcite.run;

import org.apache.calcite.tools.ValidationException;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;

import java.util.Map;
import java.util.Set;

public class SqlEngines
{
  /**
   * Validates that a provided query context does not have any of the internal, special context keys listed in the
   * {@code specialContextKeys} collection.
   *
   * Returns quietly if the context is OK; throws {@link ValidationException} if there is a problem.
   *
   * This is a helper function used by {@link SqlEngine#validateContext} implementations.
   */
  public static void validateNoSpecialContextKeys(
      final Map<String, Object> queryContext,
      final Set<String> specialContextKeys
  )
  {
    for (String contextParameterName : queryContext.keySet()) {
      if (specialContextKeys.contains(contextParameterName)) {
        throw InvalidInput.exception("Query context parameter [%s] is not allowed", contextParameterName);
      }
    }
  }

  /**
   * This is a helper function that provides a developer-friendly exception when an engine cannot recognize the feature.
   */
  public static DruidException generateUnrecognizedFeatureException(
      final String engineName,
      final EngineFeature unrecognizedFeature
  )
  {
    return DruidException
        .forPersona(DruidException.Persona.DEVELOPER)
        .ofCategory(DruidException.Category.DEFENSIVE)
        .build(
            "Engine [%s] is unable to recognize the feature [%s] for availability. This might happen when a "
            + "newer feature is added without updating all the implementations of SqlEngine(s) to either allow or disallow "
            + "its availability. Please raise an issue if you encounter this exception while using Druid.",
            engineName,
            unrecognizedFeature
        );
  }
}
