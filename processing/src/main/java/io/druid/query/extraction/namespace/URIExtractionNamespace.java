/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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
import com.metamx.common.UOE;
import com.metamx.common.parsers.CSVParser;
import com.metamx.common.parsers.DelimitedParser;
import com.metamx.common.parsers.JSONParser;
import com.metamx.common.parsers.Parser;
import io.druid.guice.annotations.Json;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
  private final FlatDataParser parseSpec;

  @JsonCreator
  public URIExtractionNamespace(
      @NotNull @JsonProperty(value = "namespace", required = true)
      String namespace,
      @NotNull @JsonProperty(value = "uri", required = true)
      URI uri,
      @JsonProperty(value = "parseSpec", required = true)
      FlatDataParser parseSpec
  )
  {
    Preconditions.checkNotNull(namespace);
    Preconditions.checkNotNull(uri);
    Preconditions.checkNotNull(parseSpec);
    this.namespace = namespace;
    this.uri = uri;
    this.parseSpec = parseSpec;
  }

  @Override
  public String getNamespace()
  {
    return namespace;
  }

  public FlatDataParser getParseSpec()
  {
    return this.parseSpec;
  }

  public URI getUri()
  {
    return uri;
  }

  @Override
  public String toString()
  {
    return String.format(
        "URIExtractionNamespace = { namespace = %s, uri = %s, parseSpec = %s }",
        namespace,
        uri.toString(),
        parseSpec.toString()
    );
  }


  private static class DelegateParser implements Parser<String, String>
  {
    private final Parser<String, Object> delegate;
    private final String key;
    private final String value;

    private DelegateParser(
        Parser<String, Object> delegate,
        String key,
        String value
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
      final String k = (String) Preconditions.checkNotNull(
          inner.get(key),
          "Key column [%s] missing data in line [%s]",
          key,
          input
      );
      final String val = (String) Preconditions.checkNotNull(
          inner.get(value),
          "Value column [%s] missing data in line [%s]",
          value,
          input
      );
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

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
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
      this.parser = new DelegateParser(new CSVParser(Optional.<String>absent(), columns), keyColumn, valueColumn);

      Preconditions.checkArgument(
          !(Strings.isNullOrEmpty(keyColumn) ^ Strings.isNullOrEmpty(valueColumn)),
          "Must specify both `keyColumn` and `valueColumn` or neither `keyColumn` nor `valueColumn`"
      );
      this.columns = columns;
      this.keyColumn = Strings.isNullOrEmpty(keyColumn) ? columns.get(0) : keyColumn;
      this.valueColumn = Strings.isNullOrEmpty(valueColumn) ? columns.get(1) : valueColumn;
      Preconditions.checkArgument(
          columns.contains(keyColumn),
          "Column [%s] not found int columns: %s",
          keyColumn,
          Arrays.toString(columns.toArray())
      );
      Preconditions.checkArgument(
          columns.contains(valueColumn),
          "Column [%s] not found int columns: %s",
          valueColumn,
          Arrays.toString(columns.toArray())
      );
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
      this.parser = new DelegateParser(delegate, keyColumn, valueColumn);
      this.columns = columns;
      this.delimiter = delimiter;
      this.keyColumn = Strings.isNullOrEmpty(keyColumn) ? columns.get(0) : keyColumn;
      this.valueColumn = Strings.isNullOrEmpty(valueColumn) ? columns.get(1) : valueColumn;
      Preconditions.checkArgument(
          columns.contains(keyColumn),
          "Column [%s] not found int columns: %s",
          keyColumn,
          Arrays.toString(columns.toArray())
      );
      Preconditions.checkArgument(
          columns.contains(valueColumn),
          "Column [%s] not found int columns: %s",
          valueColumn,
          Arrays.toString(columns.toArray())
      );
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
  }
}
