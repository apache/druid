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

package org.apache.druid.query.context;

import org.apache.druid.query.BadQueryContextException;

import javax.annotation.Nullable;
import java.util.Map;

/** Validates query context parameter values against the descriptor catalog. */
public final class QueryContextParameterValidator
{
  private QueryContextParameterValidator()
  {
  }

  /**
   * Validates a value assigned by a SQL {@code SET} statement.
   *
   * @throws BadQueryContextException if the parameter is recognized and the value is invalid
   */
  public static void validate(final String name, @Nullable final Object value)
  {
    final QueryContextParameter<?> parameter = QueryContextParameters.ALL.get().get(name);
    // Unmigrated parameters are intentionally accepted until the catalog contains every supported context parameter.
    if (parameter != null) {
      parameter.parse(value);
    }
  }

  /**
   * Validates every recognized query context parameter in the supplied map.
   *
   * @throws BadQueryContextException if a recognized parameter has an invalid value
   */
  public static void validate(final Map<String, Object> parameters)
  {
    parameters.forEach(QueryContextParameterValidator::validate);
  }
}
