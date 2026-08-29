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

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * User-facing documentation and applicability metadata for a query context parameter.
 */
public final class ParameterDocumentation
{
  public enum Language
  {
    NATIVE,
    SQL
  }

  public enum Engine
  {
    NATIVE,
    MSQ,
    DART
  }

  public enum QueryType
  {
    SCAN,
    TIMESERIES,
    TOP_N,
    GROUP_BY
  }

  public enum StatementType
  {
    SELECT,
    INSERT,
    REPLACE
  }

  private final String description;
  private final Set<Language> languages;
  private final Set<Engine> engines;
  private final Set<QueryType> queryTypes;
  private final Set<StatementType> statementTypes;
  private final Optional<String> defaultDescription;
  private final Optional<String> since;

  private ParameterDocumentation(final Builder builder)
  {
    this.description = requireNonBlank(builder.description, "description");
    this.languages = Set.copyOf(builder.languages);
    this.engines = Set.copyOf(builder.engines);
    this.queryTypes = Set.copyOf(builder.queryTypes);
    this.statementTypes = Set.copyOf(builder.statementTypes);
    this.defaultDescription = Optional.ofNullable(builder.defaultDescription);
    this.since = Optional.ofNullable(builder.since);
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public String getDescription()
  {
    return description;
  }

  public Set<Language> getLanguages()
  {
    return languages;
  }

  public Set<Engine> getEngines()
  {
    return engines;
  }

  public Set<QueryType> getQueryTypes()
  {
    return queryTypes;
  }

  public Set<StatementType> getStatementTypes()
  {
    return statementTypes;
  }

  public Optional<String> getDefaultDescription()
  {
    return defaultDescription;
  }

  public Optional<String> getSince()
  {
    return since;
  }

  private static String requireNonBlank(@Nullable final String value, final String name)
  {
    final String nonNullValue = Objects.requireNonNull(value, name);
    if (nonNullValue.isBlank()) {
      throw new IAE("Query context parameter documentation %s must not be blank", name);
    }
    return nonNullValue;
  }

  /** Not thread-safe. */
  public static final class Builder
  {
    @Nullable
    private String description;
    private final Set<Language> languages = EnumSet.noneOf(Language.class);
    private final Set<Engine> engines = EnumSet.noneOf(Engine.class);
    private final Set<QueryType> queryTypes = EnumSet.noneOf(QueryType.class);
    private final Set<StatementType> statementTypes = EnumSet.noneOf(StatementType.class);
    @Nullable
    private String defaultDescription;
    @Nullable
    private String since;

    private Builder()
    {
    }

    public Builder description(final String description)
    {
      this.description = requireNonBlank(description, "description");
      return this;
    }

    public Builder language(final Language... languages)
    {
      Collections.addAll(this.languages, languages);
      return this;
    }

    public Builder engine(final Engine... engines)
    {
      Collections.addAll(this.engines, engines);
      return this;
    }

    public Builder query(final QueryType... queryTypes)
    {
      Collections.addAll(this.queryTypes, queryTypes);
      return this;
    }

    public Builder statement(final StatementType... statementTypes)
    {
      Collections.addAll(this.statementTypes, statementTypes);
      return this;
    }

    public Builder defaultDescription(final String defaultDescription)
    {
      this.defaultDescription = requireNonBlank(defaultDescription, "default description");
      return this;
    }

    public Builder since(final String since)
    {
      this.since = requireNonBlank(since, "since");
      return this;
    }

    public ParameterDocumentation build()
    {
      return new ParameterDocumentation(this);
    }
  }
}
