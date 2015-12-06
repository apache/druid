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

package io.druid.query.extraction.namespace;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.IAE;
import com.metamx.common.UOE;
import com.metamx.common.parsers.CSVParser;
import com.metamx.common.parsers.DelimitedParser;
import com.metamx.common.parsers.JSONParser;
import com.metamx.common.parsers.Parser;
import io.druid.guice.annotations.Json;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 *
 */
@JsonTypeName("uri")
public class URIExtractionNamespace implements ExtractionNamespace
{
  @JsonProperty
  private final String namespace;
  @JsonProperty
  private final URI uri;
  @JsonProperty
  private final FlatDataParser namespaceParseSpec;
  @JsonProperty
  private final Period pollPeriod;
  @JsonProperty
  private final String versionRegex;

  @JsonCreator
  public URIExtractionNamespace(
      @NotNull @JsonProperty(value = "namespace", required = true)
      String namespace,
      @NotNull @JsonProperty(value = "uri", required = true)
      URI uri,
      @JsonProperty(value = "namespaceParseSpec", required = true)
      FlatDataParser namespaceParseSpec,
      @Min(0) @Nullable @JsonProperty(value = "pollPeriod", required = false)
      Period pollPeriod,
      @JsonProperty(value = "versionRegex", required = false)
      String versionRegex
  )
  {
    if (versionRegex != null) {
      try {
        Pattern.compile(versionRegex);
      }
      catch (PatternSyntaxException ex) {
        throw new IAE(ex, "Could not parse `versionRegex` [%s]", versionRegex);
      }
    }
    this.namespace = Preconditions.checkNotNull(namespace, "namespace");
    this.uri = Preconditions.checkNotNull(uri, "uri");
    this.namespaceParseSpec = Preconditions.checkNotNull(namespaceParseSpec, "namespaceParseSpec");
    this.pollPeriod = pollPeriod == null ? Period.ZERO : pollPeriod;

    this.versionRegex = versionRegex;
  }

  @Override
  public String getNamespace()
  {
    return namespace;
  }

  public String getVersionRegex()
  {
    return versionRegex;
  }

  public FlatDataParser getNamespaceParseSpec()
  {
    return this.namespaceParseSpec;
  }

  public URI getUri()
  {
    return uri;
  }

  @Override
  public long getPollMs()
  {
    return pollPeriod.toStandardDuration().getMillis();
  }

  @Override
  public String toString()
  {
    return String.format(
        "URIExtractionNamespace = { namespace = %s, uri = %s, namespaceParseSpec = %s, pollPeriod = %s, versionRegex = %s }",
        namespace,
        uri.toString(),
        namespaceParseSpec.toString(),
        pollPeriod.toString(),
        versionRegex
    );
  }


  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    URIExtractionNamespace namespace1 = (URIExtractionNamespace) o;
    return toString().equals(namespace1.toString());
  }

  @Override
  public int hashCode()
  {
    int result = namespace.hashCode();
    result = 31 * result + uri.hashCode();
    result = 31 * result + namespaceParseSpec.hashCode();
    result = 31 * result + pollPeriod.hashCode();
    result = 31 * result + (versionRegex != null ? versionRegex.hashCode() : 0);
    return result;
  }


  private static class DelegateParser implements Parser<String, String>
  {
    private final Parser<String, Object> delegate;
    private final String key;
    private final String value;

    private DelegateParser(
        Parser<String, Object> delegate,
        @NotNull String key,
        @NotNull String value
    )
    {
      this.delegate = delegate;
      this.key = key;
      this.value = value;
    }

    @Override
    public Map<String, String> parse(String input)
    {
      final Map<String, Object> inner = delegate.parse(input);
      final String k = Preconditions.checkNotNull(
          inner.get(key),
          "Key column [%s] missing data in line [%s]",
          key,
          input
      ).toString(); // Just in case is long
      final String val = Preconditions.checkNotNull(
          inner.get(value),
          "Value column [%s] missing data in line [%s]",
          value,
          input
      ).toString();
      return ImmutableMap.<String, String>of(k, val);
    }

    @Override
    public void setFieldNames(Iterable<String> fieldNames)
    {
      delegate.setFieldNames(fieldNames);
    }

    @Override
    public List<String> getFieldNames()
    {
      return delegate.getFieldNames();
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "format")
  @JsonSubTypes(value = {
      @JsonSubTypes.Type(name = "csv", value = CSVFlatDataParser.class),
      @JsonSubTypes.Type(name = "tsv", value = TSVFlatDataParser.class),
      @JsonSubTypes.Type(name = "customJson", value = JSONFlatDataParser.class),
      @JsonSubTypes.Type(name = "simpleJson", value = ObjectMapperFlatDataParser.class)
  })
  public static interface FlatDataParser
  {
    Parser<String, String> getParser();
  }

  @JsonTypeName("csv")
  public static class CSVFlatDataParser implements FlatDataParser
  {
    private final Parser<String, String> parser;
    private final List<String> columns;
    private final String keyColumn;
    private final String valueColumn;

    @JsonCreator
    public CSVFlatDataParser(
        @JsonProperty("columns") List<String> columns,
        @JsonProperty("keyColumn") final String keyColumn,
        @JsonProperty("valueColumn") final String valueColumn
    )
    {
      Preconditions.checkArgument(
          Preconditions.checkNotNull(columns, "`columns` list required").size() > 1,
          "Must specify more than one column to have a key value pair"
      );

      Preconditions.checkArgument(
          !(Strings.isNullOrEmpty(keyColumn) ^ Strings.isNullOrEmpty(valueColumn)),
          "Must specify both `keyColumn` and `valueColumn` or neither `keyColumn` nor `valueColumn`"
      );
      this.columns = columns;
      this.keyColumn = Strings.isNullOrEmpty(keyColumn) ? columns.get(0) : keyColumn;
      this.valueColumn = Strings.isNullOrEmpty(valueColumn) ? columns.get(1) : valueColumn;
      Preconditions.checkArgument(
          columns.contains(this.keyColumn),
          "Column [%s] not found int columns: %s",
          this.keyColumn,
          Arrays.toString(columns.toArray())
      );
      Preconditions.checkArgument(
          columns.contains(this.valueColumn),
          "Column [%s] not found int columns: %s",
          this.valueColumn,
          Arrays.toString(columns.toArray())
      );

      this.parser = new DelegateParser(new CSVParser(Optional.<String>absent(), columns), this.keyColumn, this.valueColumn);
    }

    @JsonProperty
    public List<String> getColumns()
    {
      return columns;
    }

    @JsonProperty
    public String getKeyColumn()
    {
      return this.keyColumn;
    }

    @JsonProperty
    public String getValueColumn()
    {
      return this.valueColumn;
    }

    @Override
    public Parser<String, String> getParser()
    {
      return parser;
    }

    @Override
    public String toString()
    {
      return String.format(
          "CSVFlatDataParser = { columns = %s, keyColumn = %s, valueColumn = %s }",
          Arrays.toString(columns.toArray()),
          keyColumn,
          valueColumn
      );
    }
  }

  @JsonTypeName("tsv")
  public static class TSVFlatDataParser implements FlatDataParser
  {
    private final Parser<String, String> parser;
    private final List<String> columns;
    private final String delimiter;
    private final String keyColumn;
    private final String valueColumn;

    @JsonCreator
    public TSVFlatDataParser(
        @JsonProperty("columns") List<String> columns,
        @JsonProperty("delimiter") String delimiter,
        @JsonProperty("keyColumn") final String keyColumn,
        @JsonProperty("valueColumn") final String valueColumn
    )
    {
      Preconditions.checkArgument(
          Preconditions.checkNotNull(columns, "`columns` list required").size() > 1,
          "Must specify more than one column to have a key value pair"
      );
      final DelimitedParser delegate = new DelimitedParser(
          Optional.fromNullable(Strings.isNullOrEmpty(delimiter) ? null : delimiter),
          Optional.<String>absent()
      );
      Preconditions.checkArgument(
          !(Strings.isNullOrEmpty(keyColumn) ^ Strings.isNullOrEmpty(valueColumn)),
          "Must specify both `keyColumn` and `valueColumn` or neither `keyColumn` nor `valueColumn`"
      );
      delegate.setFieldNames(columns);
      this.columns = columns;
      this.delimiter = delimiter;
      this.keyColumn = Strings.isNullOrEmpty(keyColumn) ? columns.get(0) : keyColumn;
      this.valueColumn = Strings.isNullOrEmpty(valueColumn) ? columns.get(1) : valueColumn;
      Preconditions.checkArgument(
          columns.contains(this.keyColumn),
          "Column [%s] not found int columns: %s",
          this.keyColumn,
          Arrays.toString(columns.toArray())
      );
      Preconditions.checkArgument(
          columns.contains(this.valueColumn),
          "Column [%s] not found int columns: %s",
          this.valueColumn,
          Arrays.toString(columns.toArray())
      );

      this.parser = new DelegateParser(delegate, this.keyColumn, this.valueColumn);
    }

    @JsonProperty
    public List<String> getColumns()
    {
      return columns;
    }

    @JsonProperty
    public String getKeyColumn()
    {
      return this.keyColumn;
    }

    @JsonProperty
    public String getValueColumn()
    {
      return this.valueColumn;
    }

    @JsonProperty
    public String getDelimiter()
    {
      return delimiter;
    }

    @Override
    public Parser<String, String> getParser()
    {
      return parser;
    }

    @Override
    public String toString()
    {
      return String.format(
          "TSVFlatDataParser = { columns = %s, delimiter = '%s', keyColumn = %s, valueColumn = %s }",
          Arrays.toString(columns.toArray()),
          delimiter,
          keyColumn,
          valueColumn
      );
    }
  }

  @JsonTypeName("customJson")
  public static class JSONFlatDataParser implements FlatDataParser
  {
    private final Parser<String, String> parser;
    private final String keyFieldName;
    private final String valueFieldName;

    @JsonCreator
    public JSONFlatDataParser(
        @JacksonInject @Json ObjectMapper jsonMapper,
        @JsonProperty("keyFieldName") final String keyFieldName,
        @JsonProperty("valueFieldName") final String valueFieldName
    )
    {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(keyFieldName), "[keyFieldName] cannot be empty");
      Preconditions.checkArgument(!Strings.isNullOrEmpty(valueFieldName), "[valueFieldName] cannot be empty");
      this.keyFieldName = keyFieldName;
      this.valueFieldName = valueFieldName;
      this.parser = new DelegateParser(
          new JSONParser(jsonMapper, ImmutableList.of(keyFieldName, valueFieldName)),
          keyFieldName,
          valueFieldName
      );
    }

    @JsonProperty
    public String getKeyFieldName()
    {
      return this.keyFieldName;
    }

    @JsonProperty
    public String getValueFieldName()
    {
      return this.valueFieldName;
    }

    @Override
    public Parser<String, String> getParser()
    {
      return this.parser;
    }

    @Override
    public String toString()
    {
      return String.format(
          "JSONFlatDataParser = { keyFieldName = %s, valueFieldName = %s }",
          keyFieldName,
          valueFieldName
      );
    }
  }

  @JsonTypeName("simpleJson")
  public static class ObjectMapperFlatDataParser implements FlatDataParser
  {

    private final Parser<String, String> parser;

    @JsonCreator
    public ObjectMapperFlatDataParser(
        final @JacksonInject @Json ObjectMapper jsonMapper
    )
    {
      parser = new Parser<String, String>()
      {
        @Override
        public Map<String, String> parse(String input)
        {
          try {
            return jsonMapper.readValue(
                input, new TypeReference<Map<String, String>>()
                {
                }
            );
          }
          catch (IOException e) {
            throw Throwables.propagate(e);
          }
        }

        @Override
        public void setFieldNames(Iterable<String> fieldNames)
        {
          throw new UOE("No field names available");
        }

        @Override
        public List<String> getFieldNames()
        {
          throw new UOE("No field names available");
        }
      };
    }

    @Override
    public Parser<String, String> getParser()
    {
      return parser;
    }

    @Override
    public String toString()
    {
      return "ObjectMapperFlatDataParser = { }";
    }
  }
}
