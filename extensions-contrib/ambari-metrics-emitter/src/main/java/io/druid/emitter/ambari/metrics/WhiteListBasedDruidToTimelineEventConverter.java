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

package io.druid.emitter.ambari.metrics;

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
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.metamx.common.ISE;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.java.util.common.logger.Logger;
import org.apache.hadoop.metrics2.sink.timeline.TimelineMetric;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;

@JsonTypeName("whiteList")
public class WhiteListBasedDruidToTimelineEventConverter implements DruidToTimelineMetricConverter
{
  private static final Logger LOGGER = new Logger(WhiteListBasedDruidToTimelineEventConverter.class);
  /**
   * @code whiteListDimsMapper is a white list of metric->dimensions mappings.
   * Key is the druid metric name or the metric's prefix.
   * Value is a list of metric's dimensions names.
   * The order of the dimension name is important, it will be used to build the ambari metrics name.
   * For instance if we have dimensions dataSource and queryType for metric query/time
   * the final metric name will be -> prefix.dataSource.queryType.metricName
   * Note that this name will be sanitized by replacing all the `.` or `space` to `_` {@link AmbariMetricsEmitter#sanitize(String)}
   */
  private final ImmutableSortedMap<String, ImmutableList<String>> whiteListDimsMapper;

  @JsonProperty
  private final String namespacePrefix;

  @JsonProperty
  private final String appName;

  @JsonProperty
  private final String mapPath;

  private final ObjectMapper mapper;

  @JsonCreator
  public WhiteListBasedDruidToTimelineEventConverter(
      @JsonProperty("namespacePrefix") String namespacePrefix,
      @JsonProperty("appName") String appName,
      @JsonProperty("mapPath") String mapPath,
      @JacksonInject ObjectMapper mapper
  )
  {
    this.mapper = Preconditions.checkNotNull(mapper);
    this.mapPath = mapPath;
    this.whiteListDimsMapper = readMap(this.mapPath);
    this.namespacePrefix = namespacePrefix;
    this.appName = appName == null ? SendAllTimelineEventConverter.DEFAULT_APP_NAME : appName;

  }

  @JsonProperty
  public String getNamespacePrefix()
  {
    return namespacePrefix;
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
    List<String> dimensions = whiteListDimsMapper.get(prefixKey);
    if (dimensions == null) {
      return Collections.emptyList();
    }
    for (String dimKey : dimensions) {
      String dimValue = (String) event.getUserDims().get(dimKey);
      if (dimValue != null) {
        outputList.add(AmbariMetricsEmitter.sanitize(dimValue));
      }
    }
    return outputList.build();
  }

  /**
   * @param serviceMetricEvent druid metric event to convert
   *
   * @return <tt>null</tt> if the event is not white listed, otherwise return {@link TimelineMetric}
   * <p>
   * The metric name of the ambari timeline metric event is:
   * <namespacePrefix>.<druid service name>.<white-listed dimensions>.<metric>
   * <p/>
   * The order of the dimension is the order returned by {@code getOrderedDimValues()}
   * Note that this name will be sanitized by replacing all the `.` or space by `_` {@link AmbariMetricsEmitter#sanitize(String)}
   * </p>
   */

  @Override
  public TimelineMetric druidEventToTimelineMetric(ServiceMetricEvent serviceMetricEvent)
  {
    if (!this.isInWhiteList(serviceMetricEvent)) {
      return null;
    }
    final ImmutableList.Builder<String> metricNameBuilder = new ImmutableList.Builder<>();
    if(!Strings.isNullOrEmpty(namespacePrefix)) {
      metricNameBuilder.add(namespacePrefix);
    }
    metricNameBuilder.add(AmbariMetricsEmitter.sanitize(serviceMetricEvent.getService()));
    metricNameBuilder.addAll(this.getOrderedDimValues(serviceMetricEvent));
    metricNameBuilder.add(AmbariMetricsEmitter.sanitize(serviceMetricEvent.getMetric()));

    TimelineMetric metric = new TimelineMetric();
    metric.setMetricName(Joiner.on(".").join(metricNameBuilder.build()));
    metric.setAppId(appName);
    metric.setHostName(serviceMetricEvent.getHost());
    metric.setType(serviceMetricEvent.getFeed());
    metric.setInstanceId(serviceMetricEvent.getService());
    long ts = serviceMetricEvent.getCreatedTime().getMillis();
    metric.setStartTime(ts);
    metric.setTimestamp(ts);
    metric.getMetricValues().put(ts, serviceMetricEvent.getValue().doubleValue());
    return metric;
  }

  private ImmutableSortedMap<String, ImmutableList<String>> readMap(final String mapPath)
  {
    String fileContent;
    String actualPath = mapPath;
    try {
      if (Strings.isNullOrEmpty(mapPath)) {
        actualPath = this.getClass().getClassLoader().getResource("defaultWhiteListMap.json").getFile();
        LOGGER.info("using default whiteList map located at [%s]", actualPath);
        InputStream byteContent = this.getClass().getClassLoader().getResourceAsStream("defaultWhiteListMap.json");
        fileContent = CharStreams.toString(new InputStreamReader(byteContent, StandardCharsets.UTF_8));
      } else {
        fileContent = Files.asCharSource(new File(mapPath), StandardCharsets.UTF_8).read();
      }
      return mapper.reader(new TypeReference<ImmutableSortedMap<String, ImmutableList<String>>>()
      {
      }).readValue(fileContent);
    }
    catch (IOException e) {
      throw new ISE(e, "Got an exception while parsing file [%s]", actualPath);
    }
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

    WhiteListBasedDruidToTimelineEventConverter that = (WhiteListBasedDruidToTimelineEventConverter) o;

    if (namespacePrefix != null ? !namespacePrefix.equals(that.namespacePrefix) : that.namespacePrefix != null) {
      return false;
    }
    if (!appName.equals(that.appName)) {
      return false;
    }
    return mapPath != null ? mapPath.equals(that.mapPath) : that.mapPath == null;

  }

  @Override
  public int hashCode()
  {
    int result = namespacePrefix != null ? namespacePrefix.hashCode() : 0;
    result = 31 * result + appName.hashCode();
    result = 31 * result + (mapPath != null ? mapPath.hashCode() : 0);
    return result;
  }
}
