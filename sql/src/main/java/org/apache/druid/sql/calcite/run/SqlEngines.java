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
import org.apache.druid.java.util.common.StringUtils;

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
  public static void validateNoSpecialContextKeys(final Map<String, Object> queryContext, final Set<String> specialContextKeys)
      throws ValidationException
  {
    for (String contextParameterName : queryContext.keySet()) {
      if (specialContextKeys.contains(contextParameterName)) {
        throw new ValidationException(
            StringUtils.format(
                "Cannot execute query with context parameter [%s]",
                contextParameterName
            )
        );
      }
    }
  }
}
