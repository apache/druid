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

package org.apache.druid.sql.calcite.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.query.BadQueryContextException;
import org.apache.druid.query.QueryConfigProvider;
import org.apache.druid.query.context.QueryContextParameter;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.context.constraint.ParameterConstraint;
import org.apache.druid.query.context.constraint.Range;
import org.apache.druid.query.context.docs.ParameterDocumentation;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * System table {@code sys.query_context_parameters}, listing the query context parameters declared in
 * {@link QueryContextParameters#ALL} that users can set, together with their type, default, applicability and
 * documentation. {@link QueryContextParameter.Visibility#SYSTEM} parameters, which only Druid sets, are excluded.
 */
public class SystemQueryContextParametersTable extends AbstractTable implements ScannableTable
{
  public static final String TABLE_NAME = "query_context_parameters";

  static final RowSignature ROW_SIGNATURE = RowSignature
      .builder()
      .add("name", ColumnType.STRING)
      .add("type", ColumnType.STRING)
      .add("value_range", ColumnType.STRING)
      .add("default_value", ColumnType.STRING)
      .add("effective_value", ColumnType.STRING)
      .add("effective_source", ColumnType.STRING)
      .add("default_description", ColumnType.STRING)
      .add("description", ColumnType.STRING)
      .add("visibility", ColumnType.STRING)
      .add("query_languages", ColumnType.STRING_ARRAY)
      .add("engines", ColumnType.STRING_ARRAY)
      .add("native_query_types", ColumnType.STRING_ARRAY)
      .add("statement_types", ColumnType.STRING_ARRAY)
      .add("since", ColumnType.STRING)
      .add("is_nullable", ColumnType.LONG)
      .add("deprecation_message", ColumnType.STRING)
      .build();

  /**
   * Where the effective default of a parameter comes from, in increasing precedence
   */
  enum EffectiveSource
  {
    /** The default declared with the parameter */
    BUILT_IN,
    /** A {@code druid.query.default.context.<name>} runtime property */
    RUNTIME_PROPERTY,
    /** The {@code queryContext} of the Broker dynamic configuration */
    BROKER_DYNAMIC_CONFIG,
    /** The user may not read the Broker configuration, so the effective default is not shown */
    UNAUTHORIZED
  }

  private static final Resource CONFIG_RESOURCE = new Resource("CONFIG", ResourceType.CONFIG);

  private final ObjectMapper jsonMapper;
  private final QueryConfigProvider queryConfigProvider;
  private final AuthorizerMapper authorizerMapper;
  private final AuthenticationResult authenticationResult;

  public SystemQueryContextParametersTable(
      final ObjectMapper jsonMapper,
      final QueryConfigProvider queryConfigProvider,
      final AuthorizerMapper authorizerMapper,
      final AuthenticationResult authenticationResult
  )
  {
    this.jsonMapper = jsonMapper;
    this.queryConfigProvider = queryConfigProvider;
    this.authorizerMapper = authorizerMapper;
    this.authenticationResult = authenticationResult;
  }

  @Override
  public RelDataType getRowType(final RelDataTypeFactory typeFactory)
  {
    return RowSignatures.toRelDataType(ROW_SIGNATURE, typeFactory);
  }

  @Override
  public Schema.TableType getJdbcTableType()
  {
    return Schema.TableType.SYSTEM_TABLE;
  }

  @Override
  public Enumerable<Object[]> scan(final DataContext root)
  {
    final boolean canReadConfig = canReadConfig();

    // Read once per scan, so that a Broker dynamic configuration change shows up on the next query
    final QueryConfigProvider.ContextWithOverrides contextWithOverrides = queryConfigProvider.getContextWithOverrides();
    final Map<String, Object> effectiveContext = contextWithOverrides.context();
    final Map<String, Object> dynamicContext = contextWithOverrides.dynamicOverrides();
    final List<Object[]> rows = new ArrayList<>();
    for (final QueryContextParameter<?> parameter : QueryContextParameters.ALL.get().values()) {
      if (parameter.getVisibility() != QueryContextParameter.Visibility.SYSTEM) {
        rows.add(toRow(parameter, canReadConfig, effectiveContext, dynamicContext));
      }
    }
    return Linq4j.asEnumerable(rows);
  }

  /**
   * The parameter catalog is public, but the effective defaults come from the Broker configuration, so showing them
   * requires the same permission as reading that configuration through the API.
   */
  private boolean canReadConfig()
  {
    return AuthorizationUtils.authorizeAllResourceActions(
        authenticationResult,
        Collections.singletonList(new ResourceAction(CONFIG_RESOURCE, Action.READ)),
        authorizerMapper
    ).allowAccessWithNoRestriction();
  }

  /**
   * @param canReadConfig    whether the user may see the effective default, see {@link #canReadConfig()}
   * @param effectiveContext the default query context that queries get, the runtime properties merged with the
   *                         Broker dynamic configuration
   * @param dynamicContext   the Broker dynamic configuration part of it, to tell where a value comes from
   */
  private Object[] toRow(
      final QueryContextParameter<?> parameter,
      final boolean canReadConfig,
      final Map<String, Object> effectiveContext,
      final Map<String, Object> dynamicContext
  )
  {
    final Object contextValue = effectiveContext.get(parameter.getName());
    final Object builtInValue = parameter.getDefaultValue().orElse(null);
    final Object effectiveValue;
    final EffectiveSource effectiveSource;
    if (!canReadConfig) {
      // Same for every row, so that the user can't tell which parameters the operator has configured
      effectiveValue = null;
      effectiveSource = EffectiveSource.UNAUTHORIZED;
    } else if (contextValue != null) {
      effectiveValue = parseOrRaw(parameter, contextValue);
      effectiveSource = dynamicContext.get(parameter.getName()) != null
                        ? EffectiveSource.BROKER_DYNAMIC_CONFIG
                        : EffectiveSource.RUNTIME_PROPERTY;
    } else if (builtInValue != null) {
      effectiveValue = builtInValue;
      effectiveSource = EffectiveSource.BUILT_IN;
    } else {
      effectiveValue = null;
      effectiveSource = null;
    }

    final ParameterDocumentation docs = parameter.getDocumentation();
    final Class<?> valueType = parameter.getValueType();
    return new Object[]{
        parameter.getName(),
        typeName(valueType),
        valueRange(parameter),
        builtInValue == null ? null : toJsonForm(builtInValue),
        effectiveValue == null ? null : toJsonForm(effectiveValue),
        effectiveSource == null ? null : effectiveSource.name(),
        docs.getDefaultDescription().orElse(null),
        docs.getDescription(),
        parameter.getVisibility().name(),
        names(docs.getQueries()),
        names(docs.getEngines()),
        docs.getEngines().contains(ParameterDocumentation.Engine.NATIVE) ? values(docs.getQueryTypes()) : null,
        docs.getQueries().contains(ParameterDocumentation.Query.SQL) ? names(docs.getStatementTypes()) : null,
        parameter.getSince(),
        parameter.isNullable() ? 1L : 0L,
        parameter.getDeprecationMessage()
    };
  }

  private static String typeName(final Class<?> valueType)
  {
    if (valueType.isEnum()) {
      return "ENUM";
    } else if (Boolean.class.equals(valueType)) {
      return "BOOLEAN";
    } else if (Integer.class.equals(valueType)) {
      return "INTEGER";
    } else if (Long.class.equals(valueType)) {
      return "LONG";
    } else if (Float.class.equals(valueType)) {
      return "FLOAT";
    } else if (String.class.equals(valueType)) {
      return "STRING";
    } else if (HumanReadableBytes.class.equals(valueType)) {
      return "HUMAN_READABLE_NUMBER";
    } else {
      return "OBJECT";
    }
  }

  @Nullable
  private static String valueRange(final QueryContextParameter<?> parameter)
  {
    for (final ParameterConstraint<?> constraint : parameter.getConstraints()) {
      if (constraint instanceof Range.Constraint<?> range) {
        return "[" + range.getLowerBound() + ", " + range.getUpperBound() + "]";
      }
    }
    return null;
  }

  /**
   * Returns a configured value as the parameter reads it, so that it is shown in the same form as
   * {@code default_value}. A value the parameter rejects is shown as configured.
   */
  private static Object parseOrRaw(final QueryContextParameter<?> parameter, final Object value)
  {
    try {
      return parameter.parse(value);
    }
    catch (BadQueryContextException e) {
      return value;
    }
  }

  private static List<String> names(final Collection<? extends Enum<?>> values)
  {
    return values.stream().sorted().map(Enum::name).collect(Collectors.toCollection(SortableStringList::new));
  }

  private static List<String> values(final Collection<? extends Enum<?>> values)
  {
    return values.stream().sorted().map(Enum::toString).collect(Collectors.toCollection(SortableStringList::new));
  }

  /**
   * Value of an {@code ARRAY<VARCHAR>} column. The Bindable sort casts column values to {@link Comparable}, so array
   * values must be comparable for {@code ORDER BY} on these columns to work. Arrays compare element by element, and a
   * shorter array that is a prefix of a longer one sorts first.
   */
  static final class SortableStringList extends ArrayList<String> implements Comparable<SortableStringList>
  {
    @Override
    public int compareTo(final SortableStringList other)
    {
      final int common = Math.min(size(), other.size());
      for (int i = 0; i < common; i++) {
        final int comparison = get(i).compareTo(other.get(i));
        if (comparison != 0) {
          return comparison;
        }
      }
      return Integer.compare(size(), other.size());
    }
  }

  /**
   * Returns the value as it appears in a JSON query context: strings as is, other values as JSON text.
   */
  private String toJsonForm(final Object value)
  {
    final Object json = jsonMapper.convertValue(value, Object.class);
    if (json instanceof String) {
      return (String) json;
    }
    try {
      return jsonMapper.writeValueAsString(json);
    }
    catch (JsonProcessingException e) {
      throw DruidException.defensive(e, "Cannot serialize query context value[%s]", value);
    }
  }
}
