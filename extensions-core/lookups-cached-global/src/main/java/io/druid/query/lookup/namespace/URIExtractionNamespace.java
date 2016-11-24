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
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.UOE;
import io.druid.java.util.common.parsers.CSVParser;
import io.druid.java.util.common.parsers.DelimitedParser;
import io.druid.java.util.common.parsers.JSONParser;
import io.druid.java.util.common.parsers.Parser;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 *
 */
@JsonTypeName("uri")
public class URIExtractionNamespace extends ExtractionNamespace
{
  @JsonProperty
  private final URI uri;
  @JsonProperty
  private final URI uriPrefix;
  @JsonProperty
  private final FlatDataParser namespaceParseSpec;
  @JsonProperty
  private final Period pollPeriod;
  @JsonProperty
  private final String fileRegex;

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

  public FlatDataParser getParser()
  {
    return namespaceParseSpec;
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "format")
  @JsonSubTypes(value = {
      @JsonSubTypes.Type(name = "csv", value = CSVFlatDataParser.class),
      @JsonSubTypes.Type(name = "tsv", value = TSVFlatDataParser.class),
      @JsonSubTypes.Type(name = "customJson", value = JSONFlatDataParser.class),
      @JsonSubTypes.Type(name = "simpleJson", value = ObjectMapperFlatDataParser.class)
  })
  public static abstract class FlatDataParser implements Parser<Pair, Map<String, String>>
  {
    protected String id = null;

    public FlatDataParser withID(String id)
    {
      this.id = id;
      return this;
    }

    @Override
    public Map<Pair, Map<String, String>> parse(String input)
    {
      Preconditions.checkArgument(id != null, "ID should be set before parse().");
      return parseInternal(input);
    }

    public abstract Map<Pair, Map<String, String>> parseInternal(String input);
  }

  public static class MultiMapFlatDataParser extends FlatDataParser
  {
    protected final List<KeyValueMap> maps;
    protected final Parser<String, Object> delegate;

    public MultiMapFlatDataParser(
        Parser<String, Object> delegate,
        List<KeyValueMap> maps
    )
    {
      this.delegate = delegate;
      this.maps = maps;
    }

    @Override
    public Map<Pair, Map<String, String>> parseInternal(String input)
    {
      final Map<String, Object> inner = delegate.parse(input);
      ImmutableMap.Builder<Pair, Map<String, String>> builder = new ImmutableMap.Builder<>();

      for (KeyValueMap map: maps)
      {
        final String k = Preconditions.checkNotNull(
            inner.get(map.getKeyColumn()),
            "Key column [%s] missing data in line [%s]",
            map.getKeyColumn(),
            input
        ).toString();// Just in case is long
        final Object val = inner.get(map.getValueColumn());
        // Skip null or missing values, treat them as if there were no row at all.
        if (val != null) {
          builder.put(new Pair(id, map.getMapName()), ImmutableMap.of(k, val.toString()));
        }
      }

      return builder.build();
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

    @JsonProperty
    public List<KeyValueMap> getMaps()
    {
      return maps;
    }
  }

  @JsonTypeName("csv")
  public static class CSVFlatDataParser extends MultiMapFlatDataParser
  {
    private final List<String> columns;

    @JsonCreator
    public CSVFlatDataParser(
        @JsonProperty("columns") List<String> columns,
        @JsonProperty("maps") List<KeyValueMap> maps
    )
    {
      super(
          new CSVParser(Optional.<String>absent(), columns),
          URIExtractionNamespace.getOrCreateKeyVauleMaps(maps, columns)
      );

      this.columns = columns;
    }

    @JsonProperty
    public List<String> getColumns()
    {
      return columns;
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

      if (!getMaps().containsAll(that.getMaps()) || !that.getMaps().containsAll(getMaps())) {
        return false;
      }

      return getColumns().equals(that.getColumns());
    }

    @Override
    public String toString()
    {
      return String.format(
          "CSVFlatDataParser = { " +
              "columns = %s " +
              ", maps = [" + StringUtils.join(maps, ',') + "]" +
              "}",
          Arrays.toString(columns.toArray())
      );
    }
  }

  @JsonTypeName("tsv")
  public static class TSVFlatDataParser extends MultiMapFlatDataParser
  {
    private final List<String> columns;
    private final String delimiter;
    private final String listDelimiter;

    @JsonCreator
    public TSVFlatDataParser(
        @JsonProperty("columns") List<String> columns,
        @JsonProperty("delimiter") String delimiter,
        @JsonProperty("listDelimiter") String listDelimiter,
        @JsonProperty("maps") List<KeyValueMap> maps
    )
    {
      super(
          new DelimitedParser(
              Optional.fromNullable(Strings.emptyToNull(delimiter)),
              Optional.fromNullable(Strings.emptyToNull(listDelimiter)),
              columns
          ),
          URIExtractionNamespace.getOrCreateKeyVauleMaps(maps, columns)
      );

      this.columns = columns;
      this.delimiter = delimiter;
      this.listDelimiter = listDelimiter;
    }

    @JsonProperty
    public List<String> getColumns()
    {
      return columns;
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
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TSVFlatDataParser that = (TSVFlatDataParser) o;

      if (!getMaps().containsAll(that.getMaps()) || !that.getMaps().containsAll(getMaps())) {
        return false;
      }

      if (!getColumns().equals(that.getColumns())) {
        return false;
      }
      if ((getDelimiter() == null) ? that.getDelimiter() == null : getDelimiter().equals(that.getDelimiter())) {
        return false;
      }
      return true;
    }

    @Override
    public String toString()
    {
      return String.format(
          "TSVFlatDataParser = { " +
              "columns = %s" +
              ", delimiter = '%s'" +
              ", listDelimiter = '%s'" +
              ", maps = [" + StringUtils.join(maps, ',') + "]" +
              "}",
          Arrays.toString(columns.toArray()),
          delimiter,
          listDelimiter
      );
    }
  }

  @JsonTypeName("customJson")
  public static class JSONFlatDataParser extends MultiMapFlatDataParser
  {
    @JsonCreator
    public JSONFlatDataParser(
        @JacksonInject @Json ObjectMapper jsonMapper,
        @JsonProperty("maps") List<KeyValueMap> maps
    )
    {
      super(
          new JSONParser(jsonMapper, getNeededColumns(maps)),
          maps
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

      JSONFlatDataParser that = (JSONFlatDataParser)o;

      return (getMaps().containsAll(that.getMaps()) && that.getMaps().containsAll(getMaps()));
    }

    @Override
    public String toString()
    {
      return String.format(
          "JSONFlatDataParser = { " +
              "maps = [" + StringUtils.join(maps, ',') + "]" +
              "}"
      );
    }

    private static Set<String> getNeededColumns(List<KeyValueMap> maps)
    {
      Set<String> neededColumns = Sets.newHashSet();
      Preconditions.checkArgument(maps != null, "key/value map should be specified");
      for (KeyValueMap map: maps) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(map.getKeyColumn()), "key cannot be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(map.getValueColumn()), "value cannot be empty");
        neededColumns.add(map.getKeyColumn());
        neededColumns.add(map.getValueColumn());
      }

      return neededColumns;
    }
  }

  @JsonTypeName("simpleJson")
  public static class ObjectMapperFlatDataParser extends FlatDataParser
  {
    private final ObjectMapper jsonMapper;

    @JsonCreator
    public ObjectMapperFlatDataParser(
        final @JacksonInject @Json ObjectMapper jsonMapper
    )
    {
      this.jsonMapper = jsonMapper;
    }

    @Override
    public Map<Pair, Map<String, String>> parseInternal(String input)
    {
      Preconditions.checkArgument(id != null, "ID should be set before.");

      try {
        Map<String, String> kvMap = jsonMapper.readValue(
            input, new TypeReference<Map<String, String>>() {}
        );
        return ImmutableMap.of(
            new Pair(id, KeyValueMap.DEFAULT_MAPNAME),
            kvMap
        );
      } catch (IOException e) {
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

  private static List<KeyValueMap> getOrCreateKeyVauleMaps(List<KeyValueMap> maps, List<String> columns)
  {
    Preconditions.checkArgument(
        Preconditions.checkNotNull(columns, "`columns` list required").size() > 1,
        "Must specify more than one column to have a key value pair"
    );

    if (maps == null) {
      return ImmutableList.of(new KeyValueMap(KeyValueMap.DEFAULT_MAPNAME, columns.get(0), columns.get(1)));
    }

    Set<String> neededColumns = FluentIterable
        .from(maps)
        .transformAndConcat(
            new Function<KeyValueMap, List<String>>()
            {
              @Override
              public List<String> apply(KeyValueMap input)
              {
                return ImmutableList.of(input.getKeyColumn(), input.getValueColumn());
              }
            }
        )
        .toSet();
    Preconditions.checkArgument(
        columns.containsAll(neededColumns),
        "columns should contains all the key/value columns specified in keyValueMaps: columns[%s], keyValueMaps[%s]",
        StringUtils.join(columns, ","), StringUtils.join(maps, ",")
    );

    List<KeyValueMap> defaultFilledMaps = Lists.newArrayListWithCapacity(maps.size());
    for (KeyValueMap map: maps) {
      String key = map.getKeyColumn();
      String value = map.getValueColumn();
      if (Strings.isNullOrEmpty(key)) {
        key = columns.get(0);
      }
      if (Strings.isNullOrEmpty(value)) {
        value = columns.get(1);
      }

      defaultFilledMaps.add(new KeyValueMap(map.getMapName(), key, value));
    }

    return defaultFilledMaps;
  }
}
