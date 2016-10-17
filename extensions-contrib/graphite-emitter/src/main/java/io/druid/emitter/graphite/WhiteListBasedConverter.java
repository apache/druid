/*
 *  Licensed to Metamarkets Group Inc. (Metamarkets) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. Metamarkets licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.druid.emitter.graphite;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.metamx.emitter.service.ServiceMetricEvent;

import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

@JsonTypeName("whiteList")
public class WhiteListBasedConverter implements DruidToGraphiteEventConverter
{
  private static final Logger LOGGER = new Logger(WhiteListBasedConverter.class);
  /**
   * @code whiteListDimsMapper is a white list of metric->dimensions mappings.
   * Key is the metric name or the metric's prefix.
   * Value is a list of metric's dimensions names.
   * The order of the dimension name is important, it will be used to build the graphite metric path.
   * For instance we have dimension type is nested under dimension dataSource -> prefix.dataSource.queryType.metricName
   */
  private final ImmutableSortedMap<String, ImmutableSet<String>> whiteListDimsMapper;

  @JsonProperty
  private final boolean ignoreHostname;

  @JsonProperty
  private final boolean ignoreServiceName;

  @JsonProperty
  private final String namespacePrefix;

  @JsonProperty
  private final String mapPath;

  private final ObjectMapper mapper;

  @JsonCreator
  public WhiteListBasedConverter(
      @JsonProperty("namespacePrefix") String namespacePrefix,
      @JsonProperty("ignoreHostname") Boolean ignoreHostname,
      @JsonProperty("ignoreServiceName") Boolean ignoreServiceName,
      @JsonProperty("mapPath") String mapPath,
      @JacksonInject ObjectMapper mapper
  )
  {
    this.mapper = Preconditions.checkNotNull(mapper);
    this.mapPath = mapPath;
    this.whiteListDimsMapper = readMap(this.mapPath);
    this.ignoreHostname = ignoreHostname == null ? false : ignoreHostname;
    this.ignoreServiceName = ignoreServiceName == null ? false : ignoreServiceName;
    this.namespacePrefix = Preconditions.checkNotNull(namespacePrefix, "namespace prefix can not be null");
  }

  @JsonProperty
  public boolean isIgnoreHostname()
  {
    return ignoreHostname;
  }

  @JsonProperty
  public boolean isIgnoreServiceName()
  {
    return ignoreServiceName;
  }

  @JsonProperty
  public String getNamespacePrefix()
  {
    return namespacePrefix;
  }

  public ImmutableSortedMap<String, ImmutableSet<String>> getWhiteListDimsMapper()
  {
    return whiteListDimsMapper;
  }

  /**
   * @param event Event subject to filtering
   *
   * @return true if and only if the event prefix key is in the {@code whiteListDimsMapper}
   */
  private boolean isInWhiteList(ServiceMetricEvent event)
  {
    return getPrefixKey(event.getMetric(), whiteListDimsMapper) != null;
  }

  /**
   * @param key       the metric name to lookup
   * @param whiteList
   *
   * @return <tt>null</tt> if the key does not match with any of the prefixes keys in @code metricsWhiteList,
   * or the prefix in @code whiteListDimsMapper
   */
  private String getPrefixKey(String key, SortedMap<String, ?> whiteList)
  {
    String prefixKey = null;
    if (whiteList.containsKey(key)) {
      return key;
    }
    SortedMap<String, ?> headMap = whiteList.headMap(key);
    if (!headMap.isEmpty() && key.startsWith(headMap.lastKey())) {
      prefixKey = headMap.lastKey();
    }
    return prefixKey;
  }

  /**
   * Returns a {@link List} of the white-listed dimension's values to send.
   * The list is order is the same as the order of dimensions {@code whiteListDimsMapper}
   *
   * @param event the event for which will filter dimensions
   *
   * @return {@link List}  of the filtered dimension values to send or <tt>null<tt/> if the event is not in the white list
   */
  private List<String> getOrderedDimValues(ServiceMetricEvent event)
  {
    String prefixKey = getPrefixKey(event.getMetric(), whiteListDimsMapper);
    if (prefixKey == null) {
      return null;
    }
    ImmutableList.Builder<String> outputList = new ImmutableList.Builder();
    Set<String> dimensions = whiteListDimsMapper.get(prefixKey);
    if (dimensions == null) {
      return Collections.emptyList();
    }
    for (String dimKey : dimensions) {
      String dimValue = (String) event.getUserDims().get(dimKey);
      if (dimValue != null) {
        outputList.add(GraphiteEmitter.sanitize(dimValue));
      }
    }
    return outputList.build();
  }

  /**
   * @param serviceMetricEvent druid metric event to convert
   *
   * @return <tt>null</tt> if the event is not white listed, otherwise return {@link GraphiteEvent}
   * <p>
   * The metric path of the graphite event is:
   * <namespacePrefix>.[<druid service name>].[<druid hostname>].<white-listed dimensions>.<metric>
   * <p/>
   * The order of the dimension is the order returned by {@code getOrderedDimValues()}
   * Note that this path will be sanitized by replacing all the `.` or space by `_` {@link GraphiteEmitter#sanitize(String)}
   * </p>
   */

  @Override
  public GraphiteEvent druidEventToGraphite(ServiceMetricEvent serviceMetricEvent)
  {
    if (!this.isInWhiteList(serviceMetricEvent)) {
      return null;
    }
    final ImmutableList.Builder<String> metricPathBuilder = new ImmutableList.Builder<>();
    metricPathBuilder.add(this.getNamespacePrefix());
    if (!this.isIgnoreServiceName()) {
      metricPathBuilder.add(GraphiteEmitter.sanitize(serviceMetricEvent.getService()));
    }
    if (!this.isIgnoreHostname()) {
      metricPathBuilder.add(GraphiteEmitter.sanitize(serviceMetricEvent.getHost()));
    }
    metricPathBuilder.addAll(this.getOrderedDimValues(serviceMetricEvent));
    metricPathBuilder.add(GraphiteEmitter.sanitize(serviceMetricEvent.getMetric()));

    final GraphiteEvent graphiteEvent = new GraphiteEvent(
        Joiner.on(".").join(metricPathBuilder.build()),
        String.valueOf(serviceMetricEvent.getValue()),
        TimeUnit.MILLISECONDS.toSeconds(serviceMetricEvent.getCreatedTime().getMillis())
    );
    return graphiteEvent;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof WhiteListBasedConverter)) {
      return false;
    }

    WhiteListBasedConverter that = (WhiteListBasedConverter) o;

    if (isIgnoreHostname() != that.isIgnoreHostname()) {
      return false;
    }
    if (isIgnoreServiceName() != that.isIgnoreServiceName()) {
      return false;
    }
    if (!getNamespacePrefix().equals(that.getNamespacePrefix())) {
      return false;
    }
    return mapPath != null ? mapPath.equals(that.mapPath) : that.mapPath == null;

  }

  @Override
  public int hashCode()
  {
    int result = (isIgnoreHostname() ? 1 : 0);
    result = 31 * result + (isIgnoreServiceName() ? 1 : 0);
    result = 31 * result + getNamespacePrefix().hashCode();
    result = 31 * result + (mapPath != null ? mapPath.hashCode() : 0);
    return result;
  }

  private ImmutableSortedMap<String, ImmutableSet<String>> readMap(final String mapPath)
  {
    String fileContent;
    String actualPath = mapPath;
    try {
      if (Strings.isNullOrEmpty(mapPath)) {
        actualPath = this.getClass().getClassLoader().getResource("defaultWhiteListMap.json").getFile();
        LOGGER.info("using default whiteList map located at [%s]", actualPath);
        fileContent = CharStreams.toString(new InputStreamReader(this.getClass()
                                                                     .getClassLoader()
                                                                     .getResourceAsStream("defaultWhiteListMap.json")));
      } else {
        fileContent = Files.asCharSource(new File(mapPath), Charset.forName("UTF-8")).read();
      }
      return mapper.reader(new TypeReference<ImmutableSortedMap<String, ImmutableSet<String>>>()
      {
      }).readValue(fileContent);
    }
    catch (IOException e) {
      throw new ISE(e, "Got an exception while parsing file [%s]", actualPath);
    }
  }
}
