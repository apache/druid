/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class SqlQuery
{
  public enum ResultFormat
  {
    ARRAY {
      @Override
      public void writeResultStart(final JsonGenerator jsonGenerator) throws IOException
      {
        jsonGenerator.writeStartArray();
      }

      @Override
      public void writeResultField(
          final JsonGenerator jsonGenerator,
          final String name,
          final Object value
      ) throws IOException
      {
        jsonGenerator.writeObject(value);
      }

      @Override
      public void writeResultEnd(final JsonGenerator jsonGenerator) throws IOException
      {
        jsonGenerator.writeEndArray();
      }
    },

    OBJECT {
      @Override
      public void writeResultStart(final JsonGenerator jsonGenerator) throws IOException
      {
        jsonGenerator.writeStartObject();
      }

      @Override
      public void writeResultField(
          final JsonGenerator jsonGenerator,
          final String name,
          final Object value
      ) throws IOException
      {
        jsonGenerator.writeFieldName(name);
        jsonGenerator.writeObject(value);
      }

      @Override
      public void writeResultEnd(final JsonGenerator jsonGenerator) throws IOException
      {
        jsonGenerator.writeEndObject();
      }
    };

    public abstract void writeResultStart(JsonGenerator jsonGenerator) throws IOException;

    public abstract void writeResultField(JsonGenerator jsonGenerator, String name, Object value)
        throws IOException;

    public abstract void writeResultEnd(JsonGenerator jsonGenerator) throws IOException;

    @JsonCreator
    public static ResultFormat fromString(@Nullable final String name)
    {
      if (name == null) {
        return null;
      }
      return valueOf(StringUtils.toUpperCase(name));
    }
  }

  private final String query;
  private final ResultFormat resultFormat;
  private final Map<String, Object> context;

  @JsonCreator
  public SqlQuery(
      @JsonProperty("query") final String query,
      @JsonProperty("resultFormat") final ResultFormat resultFormat,
      @JsonProperty("context") final Map<String, Object> context
  )
  {
    this.query = Preconditions.checkNotNull(query, "query");
    this.resultFormat = resultFormat == null ? ResultFormat.OBJECT : resultFormat;
    this.context = context == null ? ImmutableMap.<String, Object>of() : context;
  }

  @JsonProperty
  public String getQuery()
  {
    return query;
  }

  @JsonProperty
  public ResultFormat getResultFormat()
  {
    return resultFormat;
  }

  @JsonProperty
  public Map<String, Object> getContext()
  {
    return context;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SqlQuery sqlQuery = (SqlQuery) o;
    return Objects.equals(query, sqlQuery.query) &&
           resultFormat == sqlQuery.resultFormat &&
           Objects.equals(context, sqlQuery.context);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(query, resultFormat, context);
  }

  @Override
  public String toString()
  {
    return "SqlQuery{" +
           "query='" + query + '\'' +
           ", resultFormat=" + resultFormat +
           ", context=" + context +
           '}';
  }
}
