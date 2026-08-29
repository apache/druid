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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.context.constraint.Range;
import org.apache.druid.query.context.docs.ParameterDocumentation;
import org.apache.druid.query.context.docs.ParameterDocumentation.Engine;
import org.apache.druid.query.context.docs.ParameterDocumentation.Language;
import org.apache.druid.query.context.docs.ParameterDocumentation.QueryType;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Common query context parameter descriptors.
 */
public final class QueryContextParameters
{
  public static final QueryContextParameter<Boolean> USE_RESULT_LEVEL_CACHE = booleanParameter("useResultLevelCache")
      .defaultValue(true)
      .docs(
          doc().description(
                   """
                   Flag indicating whether to leverage the result level cache for this query. When set to false, it \
                   disables reading from the query cache for this query. When set to true, Druid uses \
                   `druid.broker.cache.useResultLevelCache` to determine whether or not to read from the \
                   result-level query cache.\
                   """
               )
               .language(Language.NATIVE, Language.SQL)
               .engine(Engine.NATIVE)
               .build()
      )
      .build();

  public static final QueryContextParameter<Integer> MAX_ROWS_QUEUED_FOR_ORDERING =
      integerParameter("maxRowsQueuedForOrdering")
          .constraint(Range.closedRange(1, Integer.MAX_VALUE))
          .docs(
              doc().description(
                       """
                       The maximum number of rows returned when time ordering is used. Overrides the identically \
                       named config.\
                       """
                   )
                   .defaultDescription("druid.query.scan.maxRowsQueuedForOrdering")
                   .language(Language.NATIVE)
                   .engine(Engine.NATIVE)
                   .query(QueryType.SCAN)
                   .build()
          )
          .build();

  /** Immutable query context parameter descriptors indexed by parameter name. */
  public static final Map<String, QueryContextParameter<?>> BY_NAME =
      Arrays.stream(QueryContextParameters.class.getDeclaredFields())
            .filter(field -> Modifier.isPublic(field.getModifiers()) && Modifier.isStatic(field.getModifiers()))
            .filter(field -> QueryContextParameter.class.equals(field.getType()))
            .map(QueryContextParameters::getParameter)
            .collect(Collectors.toUnmodifiableMap(QueryContextParameter::getName, Function.identity()));

  private QueryContextParameters()
  {
  }

  /**
   * Validates a value assigned by a SQL {@code SET} statement.
   */
  public static void validate(final String name, @Nullable final Object value)
  {
    final QueryContextParameter<?> parameter = BY_NAME.get(name);
    // Unmigrated parameters are intentionally accepted until the catalog contains every supported context parameter.
    if (parameter != null) {
      parameter.parse(value);
    }
  }

  /**
   * Validates every recognized query context parameter in the supplied map.
   */
  public static void validate(final Map<String, Object> parameters)
  {
    parameters.forEach(QueryContextParameters::validate);
  }

  private static QueryContextParameter<?> getParameter(final Field field)
  {
    try {
      return (QueryContextParameter<?>) field.get(null);
    }
    catch (final IllegalAccessException e) {
      throw new ISE(e, "Unable to read query context parameter field [%s]", field.getName());
    }
  }

  // These builders temporarily delegate to QueryContexts for its established coercion behavior. The coercion logic
  // can move into this class after all query context parameters and their callers have migrated to descriptors.
  static QueryContextParameter.Builder<Boolean> booleanParameter(final String name)
  {
    return QueryContextParameter.builder(name, Boolean.class, value -> QueryContexts.getAsBoolean(name, value));
  }

  static QueryContextParameter.Builder<Integer> integerParameter(final String name)
  {
    return QueryContextParameter.builder(name, Integer.class, value -> QueryContexts.getAsInt(name, value));
  }

  static QueryContextParameter.Builder<Long> longParameter(final String name)
  {
    return QueryContextParameter.builder(name, Long.class, value -> QueryContexts.getAsLong(name, value));
  }

  static QueryContextParameter.Builder<String> stringParameter(final String name)
  {
    return QueryContextParameter.builder(name, String.class, value -> QueryContexts.getAsString(name, value, null));
  }

  private static ParameterDocumentation.Builder doc()
  {
    return ParameterDocumentation.builder();
  }
}
