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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.BadQueryContextException;
import org.apache.druid.query.context.constraint.ParameterConstraint;
import org.apache.druid.query.context.constraint.Range;
import org.apache.druid.query.context.docs.ParameterDocumentation;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Describes a query context parameter without changing how existing query code reads the context. Instances are
 * immutable and thread-safe when their parser and constraint implementations are thread-safe.
 *
 * <p>A {@code null} value means "unset": readers treat an explicit {@code null} the same as an absent key, and writers
 * remove the key (or store {@code null} where removal is not possible). {@link #isNullable()} controls whether a
 * {@code null} may be supplied at all. Invalid values are reported with {@link BadQueryContextException}.</p>
 *
 * @param <T> parsed value type
 */
public final class QueryContextParameter<T>
{
  @FunctionalInterface
  public interface ValueParser<T>
  {
    /**
     * Converts a raw context value into the parameter type. Values that are already instances of the parameter's value
     * type bypass the parser, so implementations only need to handle values that require coercion, such as strings.
     * {@code null} values are handled before the parser is invoked. Implementations must be thread-safe and must throw
     * {@link BadQueryContextException} for values that cannot be converted.
     */
    @Nullable
    T parse(Object value);
  }

  /**
   * Thrown by {@link #exactNumber} for a whole number that does not fit the value type. {@link #parse(Object)}
   * converts it to a {@link BadQueryContextException} that quotes the parameter's {@link Range} bounds when declared.
   */
  private static final class OutOfTypeRangeException extends RuntimeException
  {
    private final Object value;
    private final long min;
    private final long max;

    OutOfTypeRangeException(final Object value, final long min, final long max)
    {
      super(null, null, false, false);
      this.value = value;
      this.min = min;
      this.max = max;
    }
  }

  private final String name;
  private final Class<T> valueType;
  private final ValueParser<T> parser;
  private final List<ParameterConstraint<T>> constraints;
  private final Optional<T> defaultValue;
  private final boolean nullable;
  private final Optional<String> deprecationMessage;
  private final Optional<ParameterDocumentation> documentation;

  private QueryContextParameter(final Builder<T> builder)
  {
    this.name = builder.name;
    this.valueType = builder.valueType;
    this.parser = builder.parser;
    this.constraints = List.copyOf(builder.constraints);
    this.defaultValue = Optional.ofNullable(builder.defaultValue);
    this.nullable = builder.nullable;
    this.deprecationMessage = Optional.ofNullable(builder.deprecationMessage);
    this.documentation = builder.documentationBuilder == null
                         ? Optional.empty()
                         : Optional.of(builder.documentationBuilder.build());

    if (defaultValue.isPresent()) {
      try {
        validate(defaultValue.get());
      }
      catch (BadQueryContextException e) {
        // An invalid declared default is a programming error, not a bad user-supplied value.
        throw new IAE(e, "Invalid default value for query context parameter [%s]: %s", name, e.getMessage());
      }
    }
  }

  public static <T> Builder<T> builder(
      final String name,
      final Class<T> valueType,
      final ValueParser<T> parser
  )
  {
    return new Builder<>(name, valueType, parser);
  }

  public String getName()
  {
    return name;
  }

  public Class<T> getValueType()
  {
    return valueType;
  }

  /**
   * Sets this parameter in a mutable query context map. A {@code null} value removes the key.
   *
   * @throws BadQueryContextException if the value is invalid
   */
  public void set(final Map<String, Object> context, @Nullable final T value)
  {
    if (validate(value) == null) {
      context.remove(name);
    } else {
      context.put(name, value);
    }
  }

  /**
   * Converts a raw context value, such as one supplied in a JSON request or a SQL {@code SET} statement, into the
   * parameter type and validates it. The declared default is not applied.
   *
   * @return the parsed value, or {@code null} if the raw value is {@code null}
   * @throws BadQueryContextException if the value cannot be converted or violates a constraint
   */
  @Nullable
  public T parse(@Nullable final Object value)
  {
    if (value == null) {
      return validate(null);
    }
    if (valueType.isInstance(value)) {
      return validate(valueType.cast(value));
    }
    final T parsed;
    try {
      parsed = parser.parse(value);
    }
    catch (OutOfTypeRangeException e) {
      throw outOfRangeException(e);
    }
    return validate(parsed);
  }

  /**
   * Reports a value that does not fit the value type using the parameter's own {@link Range} bounds when declared, so
   * users see the same bounds regardless of whether the value overflowed the type or only violated the constraint.
   */
  private BadQueryContextException outOfRangeException(final OutOfTypeRangeException e)
  {
    for (final ParameterConstraint<T> constraint : constraints) {
      if (constraint instanceof Range.Constraint<T> range) {
        return Range.outOfRangeException(name, range.getLowerBound(), range.getUpperBound(), e.value);
      }
    }
    return Range.outOfRangeException(name, e.min, e.max, e.value);
  }

  /**
   * Parses a raw value and returns the declared default when parsing produces {@code null}.
   *
   * @throws ISE if this parameter has no declared default
   */
  public T parseOrDefault(@Nullable final Object value)
  {
    final T parsed = parse(value);
    if (parsed != null) {
      return parsed;
    }
    return defaultValue.orElseThrow(
        () -> new ISE("Query context parameter [%s] has no declared default", name)
    );
  }

  /**
   * Validates and returns an already-typed value without invoking the parser.
   *
   * @throws BadQueryContextException if the value is {@code null} for a non-nullable parameter or violates a constraint
   */
  @Nullable
  public T validate(@Nullable final T value)
  {
    if (value == null) {
      if (!nullable) {
        throw new BadQueryContextException(
            StringUtils.format("Query context parameter [%s] must not be null", name)
        );
      }
      return null;
    }
    constraints.forEach(constraint -> constraint.validate(name, value));
    return value;
  }

  public List<ParameterConstraint<T>> getConstraints()
  {
    return constraints;
  }

  /**
   * Returns the declared default, if one exists. Parameters whose fallback is supplied by runtime configuration or an
   * individual call site do not have a default value in the descriptor.
   */
  public Optional<T> getDefaultValue()
  {
    return defaultValue;
  }

  public boolean isNullable()
  {
    return nullable;
  }

  public boolean isDeprecated()
  {
    return deprecationMessage.isPresent();
  }

  public Optional<String> getDeprecationMessage()
  {
    return deprecationMessage;
  }

  public Optional<ParameterDocumentation> getDocumentation()
  {
    return documentation;
  }

  @Override
  public String toString()
  {
    return name;
  }

  /**
   * Converts a number or numeric string without silent truncation or overflow, unlike {@link Number#intValue()} and
   * {@link Number#longValue()}. Integral values with a zero fractional part, such as {@code 12.0} or {@code "12.00"},
   * are accepted. Whole numbers outside {@code [min, max]} are reported through
   * {@link OutOfTypeRangeException} so the parameter can quote its own range constraint.
   */
  static <T> T exactNumber(
      final String name,
      final Object value,
      final String typeName,
      final long min,
      final long max,
      final Function<BigDecimal, T> converter
  )
  {
    final String expectedFormat = "in " + typeName + " format";
    final BigDecimal decimal;
    try {
      if (value instanceof BigDecimal) {
        decimal = (BigDecimal) value;
      } else if (value instanceof Number || value instanceof String) {
        decimal = new BigDecimal(value.toString());
      } else {
        throw QueryContextParameters.invalidValueException(name, expectedFormat, value);
      }
    }
    catch (NumberFormatException e) {
      throw QueryContextParameters.invalidValueException(name, expectedFormat, value);
    }

    if (decimal.signum() != 0 && decimal.stripTrailingZeros().scale() > 0) {
      throw QueryContextParameters.invalidValueException(name, expectedFormat, value);
    }
    if (decimal.compareTo(BigDecimal.valueOf(min)) < 0 || decimal.compareTo(BigDecimal.valueOf(max)) > 0) {
      throw new OutOfTypeRangeException(value, min, max);
    }
    return converter.apply(decimal);
  }

  /** Not thread-safe. */
  public static final class Builder<T>
  {
    private final String name;
    private final Class<T> valueType;
    private final ValueParser<T> parser;
    private final List<ParameterConstraint<T>> constraints = new ArrayList<>();
    @Nullable
    private T defaultValue;
    // Query context maps historically permit explicit null values, so preserve that behavior unless declared otherwise.
    private boolean nullable = true;
    @Nullable
    private String deprecationMessage;
    @Nullable
    private ParameterDocumentation.Builder documentationBuilder;

    private Builder(final String name, final Class<T> valueType, final ValueParser<T> parser)
    {
      this.name = Objects.requireNonNull(name, "name");
      this.valueType = Objects.requireNonNull(valueType, "valueType");
      this.parser = Objects.requireNonNull(parser, "parser");

      if (name.isBlank() || !name.equals(name.trim())) {
        throw new IAE("Query context parameter name [%s] must not be blank or contain surrounding whitespace", name);
      }
    }

    public Builder<T> constraint(final ParameterConstraint<T> constraint)
    {
      constraints.add(Objects.requireNonNull(constraint, "constraint"));
      return this;
    }

    public Builder<T> defaultValue(final T defaultValue)
    {
      this.defaultValue = Objects.requireNonNull(defaultValue, "defaultValue");
      return this;
    }

    public Builder<T> nullable(final boolean nullable)
    {
      this.nullable = nullable;
      return this;
    }

    public Builder<T> deprecated(final String deprecationMessage)
    {
      this.deprecationMessage = Objects.requireNonNull(deprecationMessage, "deprecationMessage");
      if (deprecationMessage.isBlank()) {
        throw new IAE("Query context parameter deprecation message must not be blank");
      }
      return this;
    }

    private ParameterDocumentation.Builder documentationBuilder()
    {
      if (documentationBuilder == null) {
        documentationBuilder = ParameterDocumentation.builder();
      }
      return documentationBuilder;
    }

    public Builder<T> description(final String description)
    {
      documentationBuilder().description(description);
      return this;
    }

    public Builder<T> query(final ParameterDocumentation.Query... queries)
    {
      documentationBuilder().query(queries);
      return this;
    }

    public Builder<T> engine(final ParameterDocumentation.Engine... engines)
    {
      documentationBuilder().engine(engines);
      return this;
    }

    public Builder<T> queryType(final ParameterDocumentation.QueryType... queryTypes)
    {
      documentationBuilder().queryType(queryTypes);
      return this;
    }

    public Builder<T> statement(final ParameterDocumentation.StatementType... statementTypes)
    {
      documentationBuilder().statement(statementTypes);
      return this;
    }

    public Builder<T> defaultDescription(final String defaultDescription)
    {
      documentationBuilder().defaultDescription(defaultDescription);
      return this;
    }

    public Builder<T> since(final String since)
    {
      documentationBuilder().since(since);
      return this;
    }

    public QueryContextParameter<T> build()
    {
      return new QueryContextParameter<>(this);
    }
  }
}
