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

import org.apache.druid.java.util.common.IAE;
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
   */
  public static void validate(final String name, @Nullable final Object value)
  {
    final QueryContextParameter<?> parameter = QueryContextParameters.BY_NAME.get(name);
    // Unmigrated parameters are intentionally accepted until the catalog contains every supported context parameter.
    if (parameter != null) {
      try {
        parameter.parse(value);
      }
      catch (BadQueryContextException e) {
        throw new IAE(e, "Invalid query context parameter [%s]: %s", name, e.getMessage());
      }
    }
  }

  /**
   * Validates every recognized query context parameter in the supplied map.
   */
  public static void validate(final Map<String, Object> parameters)
  {
    parameters.forEach(QueryContextParameterValidator::validate);
  }
}
