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

package io.druid.query.lookup.namespace;

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

import io.druid.guice.annotations.Json;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.UOE;
import io.druid.java.util.common.parsers.CSVParser;
import io.druid.java.util.common.parsers.DelimitedParser;
import io.druid.java.util.common.parsers.JSONParser;
import io.druid.java.util.common.parsers.Parser;

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
  private final URI uri;
  @JsonProperty
  private final URI uriPrefix;
  @JsonProperty
  private final FlatDataParser namespaceParseSpec;
  @JsonProperty
  private final String fileRegex;
  @JsonProperty
  private final Period pollPeriod;

  @JsonCreator
  public URIExtractionNamespace(
      @JsonProperty(value = "uri", required = false)
          URI uri,
      @JsonProperty(value = "uriPrefix", required = false)
          URI uriPrefix,
      @JsonProperty(value = "fileRegex", required = false)
          String fileRegex,
      @JsonProperty(value = "namespaceParseSpec", required = true)
          FlatDataParser namespaceParseSpec,
      @Min(0) @Nullable @JsonProperty(value = "pollPeriod", required = false)
          Period pollPeriod,
      @Deprecated
      @JsonProperty(value = "versionRegex", required = false)
          String versionRegex
  )
  {
    this.uri = uri;
    this.uriPrefix = uriPrefix;
    if ((uri != null) == (uriPrefix != null)) {
      throw new IAE("Either uri xor uriPrefix required");
    }
    this.namespaceParseSpec = Preconditions.checkNotNull(namespaceParseSpec, "namespaceParseSpec");
    this.pollPeriod = pollPeriod == null ? Period.ZERO : pollPeriod;
    this.fileRegex = fileRegex == null ? versionRegex : fileRegex;
    if (fileRegex != null && versionRegex != null) {
      throw new IAE("Cannot specify both versionRegex and fileRegex. versionRegex is deprecated");
    }

    if (uri != null && this.fileRegex != null) {
      throw new IAE("Cannot define both uri and fileRegex");
    }

    if (this.fileRegex != null) {
      try {
        Pattern.compile(this.fileRegex);
      }
      catch (PatternSyntaxException ex) {
        throw new IAE(ex, "Could not parse `fileRegex` [%s]", this.fileRegex);
      }
    }
  }

  public String getFileRegex()
  {
    return fileRegex;
  }

  public FlatDataParser getNamespaceParseSpec()
  {
    return this.namespaceParseSpec;
  }

  public URI getUri()
  {
    return uri;
  }

  public URI getUriPrefix()
  {
    return uriPrefix;
  }

  @Override
  public long getPollMs()
  {
    return pollPeriod.toStandardDuration().getMillis();
  }

  @Override
  public String toString()
  {
    return "URIExtractionNamespace{" +
           "uri=" + uri +
           ", uriPrefix=" + uriPrefix +
           ", namespaceParseSpec=" + namespaceParseSpec +
           ", fileRegex='" + fileRegex + '\'' +
           ", pollPeriod=" + pollPeriod +
           '}';
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

    URIExtractionNamespace that = (URIExtractionNamespace) o;

    if (getUri() != null ? !getUri().equals(that.getUri()) : that.getUri() != null) {
      return false;
    }
    if (getUriPrefix() != null ? !getUriPrefix().equals(that.getUriPrefix()) : that.getUriPrefix() != null) {
      return false;
    }
    if (!getNamespaceParseSpec().equals(that.getNamespaceParseSpec())) {
      return false;
    }
    if (getFileRegex() != null ? !getFileRegex().equals(that.getFileRegex()) : that.getFileRegex() != null) {
      return false;
    }
    return pollPeriod.equals(that.pollPeriod);

  }

  @Override
  public int hashCode()
  {
    int result = getUri() != null ? getUri().hashCode() : 0;
    result = 31 * result + (getUriPrefix() != null ? getUriPrefix().hashCode() : 0);
    result = 31 * result + getNamespaceParseSpec().hashCode();
    result = 31 * result + (getFileRegex() != null ? getFileRegex().hashCode() : 0);
    result = 31 * result + pollPeriod.hashCode();
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

      this.parser = new DelegateParser(
          new CSVParser(Optional.<String>absent(), columns),
          this.keyColumn,
          this.valueColumn
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

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      CSVFlatDataParser that = (CSVFlatDataParser) o;

      if (!getColumns().equals(that.getColumns())) {
        return false;
      }
      if (!getKeyColumn().equals(that.getKeyColumn())) {
        return false;
      }

      return getValueColumn().equals(that.getValueColumn());
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
    private final String listDelimiter;
    private final String keyColumn;
    private final String valueColumn;

    @JsonCreator
    public TSVFlatDataParser(
        @JsonProperty("columns") List<String> columns,
        @JsonProperty("delimiter") String delimiter,
        @JsonProperty("listDelimiter") String listDelimiter,
        @JsonProperty("keyColumn") final String keyColumn,
        @JsonProperty("valueColumn") final String valueColumn
    )
    {
      Preconditions.checkArgument(
          Preconditions.checkNotNull(columns, "`columns` list required").size() > 1,
          "Must specify more than one column to have a key value pair"
      );
      final DelimitedParser delegate = new DelimitedParser(
          Optional.fromNullable(Strings.emptyToNull(delimiter)),
          Optional.fromNullable(Strings.emptyToNull(listDelimiter))
      );
      Preconditions.checkArgument(
          !(Strings.isNullOrEmpty(keyColumn) ^ Strings.isNullOrEmpty(valueColumn)),
          "Must specify both `keyColumn` and `valueColumn` or neither `keyColumn` nor `valueColumn`"
      );
      delegate.setFieldNames(columns);
      this.columns = columns;
      this.delimiter = delimiter;
      this.listDelimiter = listDelimiter;
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
    public String getListDelimiter()
    {
      return listDelimiter;
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
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TSVFlatDataParser that = (TSVFlatDataParser) o;

      if (!getColumns().equals(that.getColumns())) {
        return false;
      }
      if ((getDelimiter() == null) ? that.getDelimiter() == null : getDelimiter().equals(that.getDelimiter())) {
        return false;
      }
      if (!getKeyColumn().equals(that.getKeyColumn())) {
        return false;
      }

      return getValueColumn().equals(that.getValueColumn());
    }

    @Override
    public String toString()
    {
      return String.format(
          "TSVFlatDataParser = { columns = %s, delimiter = '%s', listDelimiter = '%s',keyColumn = %s, valueColumn = %s }",
          Arrays.toString(columns.toArray()),
          delimiter,
          listDelimiter,
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
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      JSONFlatDataParser that = (JSONFlatDataParser) o;

      if (!getKeyFieldName().equals(that.getKeyFieldName())) {
        return false;
      }

      return getValueFieldName().equals(that.getValueFieldName());
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
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      return true;
    }

    @Override
    public String toString()
    {
      return "ObjectMapperFlatDataParser = { }";
    }
  }
}
