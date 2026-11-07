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

package org.apache.druid.query.context.docs;

import org.apache.druid.java.util.common.IAE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Selects query context parameters by their {@link ParameterDocumentation} metadata. Documents use filters in their
 * generated-content markers to choose which parameters to render, so that the documents, not the generator, decide how
 * parameters are grouped.
 * <p>
 * A filter is either {@code *}, which {@link #isRemainder() stands for} the parameters that no other full-entry
 * ({@code PARAMETERS}) marker in the same document selects, or a whitespace-separated list of conditions that must all
 * hold. A condition is
 * {@code field operator values}, where {@code field} is one of {@code queries}, {@code engines}, {@code queryTypes} or
 * {@code statements}, {@code values} is a comma-separated list of constant names of the field's enum, and
 * {@code operator} is one of:
 * <ul>
 *   <li>{@code =}: the field holds exactly the values. {@code queryTypes=} selects parameters without query types.</li>
 *   <li>{@code ~}: the field holds all the values, and possibly others.</li>
 *   <li>{@code !~}: the field holds none of the values.</li>
 * </ul>
 * For example, {@code queryTypes= engines~MSQ engines!~NATIVE} selects parameters that apply to no specific query type,
 * and that the MSQ engine honors but the native engine does not.
 * <p>
 * The condition {@code names=name1,name2} selects parameters by name, for documents about a topic that the metadata
 * does not capture, such as vectorization. It only supports the {@code =} operator.
 */
final class ParameterFilter
{
  private static final String REMAINDER = "*";
  private static final String NAMES = "names";
  private static final Pattern CONDITION = Pattern.compile("^(\\w+)(=|~|!~)([\\w,]*)$");

  private final String expression;
  private final List<BiPredicate<String, ParameterDocumentation>> conditions;
  private final Set<String> names;

  private ParameterFilter(
      final String expression,
      final List<BiPredicate<String, ParameterDocumentation>> conditions,
      final Set<String> names
  )
  {
    this.expression = expression;
    this.conditions = conditions;
    this.names = names;
  }

  static ParameterFilter parse(final String expression)
  {
    final String trimmed = expression.trim();
    if (REMAINDER.equals(trimmed)) {
      return new ParameterFilter(trimmed, List.of(), Set.of());
    }
    if (trimmed.isEmpty()) {
      throw new IAE("Query context parameter filter must not be empty");
    }

    final List<BiPredicate<String, ParameterDocumentation>> conditions = new ArrayList<>();
    final Set<String> names = new HashSet<>();
    for (final String condition : trimmed.split("\\s+")) {
      final Matcher matcher = CONDITION.matcher(condition);
      if (!matcher.matches()) {
        throw new IAE("Invalid condition [%s] in query context parameter filter [%s]", condition, trimmed);
      }
      if (NAMES.equals(matcher.group(1))) {
        final Set<String> conditionNames = parseNames(condition, matcher.group(2), matcher.group(3), trimmed);
        names.addAll(conditionNames);
        conditions.add((name, docs) -> conditionNames.contains(name));
        continue;
      }
      final Field field = Field.of(matcher.group(1), trimmed);
      final Set<Enum<?>> values = field.parseValues(matcher.group(3), trimmed);
      final String operator = matcher.group(2);
      if (values.isEmpty() && !"=".equals(operator)) {
        throw new IAE("Condition [%s] in query context parameter filter [%s] has no values", condition, trimmed);
      }
      final BiPredicate<String, ParameterDocumentation> predicate = switch (operator) {
        case "=" -> (name, docs) -> field.get(docs).equals(values);
        case "~" -> (name, docs) -> field.get(docs).containsAll(values);
        default -> (name, docs) -> Collections.disjoint(field.get(docs), values);
      };
      conditions.add(predicate);
    }
    return new ParameterFilter(trimmed, conditions, names);
  }

  private static Set<String> parseNames(
      final String condition,
      final String operator,
      final String values,
      final String expression
  )
  {
    if (!"=".equals(operator)) {
      throw new IAE("Condition [%s] in query context parameter filter [%s] must use [=]", condition, expression);
    }
    final Set<String> names = new HashSet<>();
    for (final String name : values.split(",", -1)) {
      if (name.isEmpty()) {
        throw new IAE("Condition [%s] in query context parameter filter [%s] has an empty name", condition, expression);
      }
      names.add(name);
    }
    return names;
  }

  /**
   * Whether this filter stands for the parameters that no other full-entry ({@code PARAMETERS}) marker in the same
   * document selects. Such a filter {@link #matches} no parameter by itself.
   */
  boolean isRemainder()
  {
    return REMAINDER.equals(expression);
  }

  /**
   * Returns the parameter names that {@code names} conditions in this filter refer to, so that callers can check that
   * they exist.
   */
  Set<String> getNames()
  {
    return names;
  }

  boolean matches(final String name, final ParameterDocumentation docs)
  {
    return !isRemainder() && conditions.stream().allMatch(condition -> condition.test(name, docs));
  }

  @Override
  public String toString()
  {
    return expression;
  }

  private enum Field
  {
    QUERIES("queries", ParameterDocumentation.Query.class, ParameterDocumentation::getQueries),
    ENGINES("engines", ParameterDocumentation.Engine.class, ParameterDocumentation::getEngines),
    QUERY_TYPES("queryTypes", ParameterDocumentation.QueryType.class, ParameterDocumentation::getQueryTypes),
    STATEMENTS("statements", ParameterDocumentation.StatementType.class, ParameterDocumentation::getStatementTypes);

    private final String name;
    private final Class<? extends Enum<?>> type;
    private final Function<ParameterDocumentation, Set<? extends Enum<?>>> getter;

    Field(
        final String name,
        final Class<? extends Enum<?>> type,
        final Function<ParameterDocumentation, Set<? extends Enum<?>>> getter
    )
    {
      this.name = name;
      this.type = type;
      this.getter = getter;
    }

    static Field of(final String name, final String expression)
    {
      for (final Field field : values()) {
        if (field.name.equals(name)) {
          return field;
        }
      }
      throw new IAE("Unknown field [%s] in query context parameter filter [%s]", name, expression);
    }

    Set<? extends Enum<?>> get(final ParameterDocumentation docs)
    {
      return getter.apply(docs);
    }

    Set<Enum<?>> parseValues(final String values, final String expression)
    {
      final Set<Enum<?>> parsed = new HashSet<>();
      if (values.isEmpty()) {
        return parsed;
      }
      for (final String value : values.split(",", -1)) {
        parsed.add(parseValue(value, expression));
      }
      return parsed;
    }

    private Enum<?> parseValue(final String value, final String expression)
    {
      for (final Enum<?> constant : type.getEnumConstants()) {
        if (constant.name().equals(value)) {
          return constant;
        }
      }
      throw new IAE("Unknown %s value [%s] in query context parameter filter [%s]", name, value, expression);
    }
  }
}
