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

import com.google.common.base.Suppliers;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.BadQueryContextException;
import org.apache.druid.query.context.constraint.Range;
import org.apache.druid.query.context.docs.ParameterDocumentation.Engine;
import org.apache.druid.query.context.docs.ParameterDocumentation.Query;
import org.apache.druid.query.context.docs.ParameterDocumentation.QueryType;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/// Central catalog of query context parameter descriptors.
///
/// Define user-facing metadata here, including descriptions, default values, constraints,
/// applicability, and introduction versions. The
/// {@link org.apache.druid.query.context.docs.ParameterDocumentationGenerator} generates the
/// corresponding tables in `query-context-reference.md`, `scan-query.md`, and
/// `sql-query-context.md`.
///
/// When adding or changing a descriptor, including introducing a new parameter, run this command
/// from the repository root to regenerate the checked-in documentation. The default Maven mode
/// verifies that the generated documentation is up to date.
///
/// ```shell
/// mvn -ntp -pl processing -am -Pskip-static-checks -Dweb.console.skip=true -DskipTests -Dquery.context.docs.mode=generate -T1C process-classes
/// ```
public final class QueryContextParameters
{
  public static final QueryContextParameter<Boolean> USE_RESULT_LEVEL_CACHE = booleanParameter("useResultLevelCache")
      .defaultValue(true)
      .description(
          """
          Flag indicating whether to leverage the result level cache for this query.
          When set to false, it disables reading from the query cache for this query.
          When set to true, Druid uses `druid.broker.cache.useResultLevelCache` to determine whether or not to read from the result-level query cache.
          """
      )
      .since("0.13.0-incubating")
      .query(Query.JSON, Query.SQL)
      .engine(Engine.NATIVE)
      .build();

  public static final QueryContextParameter<Integer> MAX_ROWS_QUEUED_FOR_ORDERING =
      integerParameter("maxRowsQueuedForOrdering")
          .constraint(Range.closedRange(1, Integer.MAX_VALUE))
          .description(
              """
              The maximum number of rows returned when time ordering is used.
              Overrides the identically named config.
              """
          )
          .since("0.15.0-incubating")
          .defaultDescription("`druid.query.scan.maxRowsQueuedForOrdering`")
          .query(Query.JSON)
          .engine(Engine.NATIVE)
          .queryType(QueryType.SCAN)
          .build();

  private QueryContextParameters()
  {
  }

  /**
   * Immutable query context parameter descriptors indexed by parameter name. Built lazily on first access so that
   * every parameter field is initialized regardless of where it is declared in this class.
   */
  public static final Supplier<Map<String, QueryContextParameter<?>>> ALL =
      Suppliers.memoize(() -> {
        final Map<String, QueryContextParameter<?>> byName = new LinkedHashMap<>();
        for (final Field field : QueryContextParameters.class.getDeclaredFields()) {
          if (!Modifier.isPublic(field.getModifiers())
              || !Modifier.isStatic(field.getModifiers())
              || !QueryContextParameter.class.equals(field.getType())) {
            continue;
          }

          final QueryContextParameter<?> parameter;
          try {
            parameter = (QueryContextParameter<?>) field.get(null);
          }
          catch (final IllegalAccessException e) {
            throw new ISE(e, "Unable to read query context parameter field [%s]", field.getName());
          }

          final QueryContextParameter<?> existing = byName.putIfAbsent(parameter.getName(), parameter);
          if (existing != null) {
            throw new ISE(
                "Duplicate query context parameter [%s] declared by field [%s]",
                parameter.getName(),
                field.getName()
            );
          }
        }
        return Collections.unmodifiableMap(byName);
      });

  static QueryContextParameter.Builder<Boolean> booleanParameter(final String name)
  {
    return QueryContextParameter.builder(
        name,
        Boolean.class,
        value -> {
          if (value instanceof String) {
            // Matches the established QueryContexts coercion: any string other than "true" (ignoring case) is false.
            return Boolean.parseBoolean((String) value);
          }
          throw invalidValueException(name, "a boolean", value);
        }
    );
  }

  static QueryContextParameter.Builder<Integer> integerParameter(final String name)
  {
    return QueryContextParameter.builder(
        name,
        Integer.class,
        value -> QueryContextParameter.exactNumber(
            name,
            value,
            "integer",
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            BigDecimal::intValueExact
        )
    );
  }

  static QueryContextParameter.Builder<Long> longParameter(final String name)
  {
    return QueryContextParameter.builder(
        name,
        Long.class,
        value -> QueryContextParameter.exactNumber(
            name,
            value,
            "long",
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            BigDecimal::longValueExact
        )
    );
  }

  static QueryContextParameter.Builder<String> stringParameter(final String name)
  {
    return QueryContextParameter.builder(
        name,
        String.class,
        value -> {
          throw invalidValueException(name, "a string", value);
        }
    );
  }

  /**
   * Creates the exception that parsers throw for a value that cannot be converted to the parameter type.
   *
   * @param expected description of the expected value that completes "should be ...", such as "in integer format"
   */
  public static BadQueryContextException invalidValueException(
      final String name,
      final String expected,
      final Object actual
  )
  {
    return new BadQueryContextException(
        StringUtils.format("Query context parameter [%s] should be %s, but got [%s]", name, expected, actual)
    );
  }
}
