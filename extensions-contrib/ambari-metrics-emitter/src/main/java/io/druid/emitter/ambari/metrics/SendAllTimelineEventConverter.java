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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.metamx.emitter.service.ServiceMetricEvent;
import org.apache.hadoop.metrics2.sink.timeline.TimelineMetric;

/**
 * Emits all the events instance of {@link ServiceMetricEvent}.
 * <p>
 * All the dimensions will be retained and lexicographically order using dimensions name.
 * <p>
 * The metric name of the timeline event is:
 * <namespacePrefix>.<druid service name>.<dimensions values ordered by dimension's name>.<metric>
 * <p>
 * Note that this name will be sanitized by replacing all the `.` or `space` to `_` {@link AmbariMetricsEmitter#sanitize(String)}
 */

@JsonTypeName("all")
public class SendAllTimelineEventConverter implements DruidToTimelineMetricConverter
{
  public static final String DEFAULT_APP_NAME = "druid";

  @JsonProperty
  private final String namespacePrefix;

  @JsonProperty
  private final String appName;

  @JsonProperty
  public String getNamespacePrefix()
  {
    return namespacePrefix;
  }

  @JsonCreator
  public SendAllTimelineEventConverter(
      @JsonProperty("namespacePrefix") String namespacePrefix,
      @JsonProperty("appName") String appName
  )
  {
    this.namespacePrefix = namespacePrefix;
    this.appName = appName == null ? DEFAULT_APP_NAME : appName;
  }

  @Override
  public TimelineMetric druidEventToTimelineMetric(ServiceMetricEvent serviceMetricEvent)
  {
    ImmutableList.Builder metricNameBuilder = new ImmutableList.Builder<>();
    if(!Strings.isNullOrEmpty(namespacePrefix)) {
      metricNameBuilder.add(namespacePrefix);
    }
    metricNameBuilder.add(AmbariMetricsEmitter.sanitize(serviceMetricEvent.getService()));
    ImmutableSortedSet<String> dimNames = ImmutableSortedSet.copyOf(serviceMetricEvent.getUserDims().keySet());
    for (String dimName : dimNames) {
      metricNameBuilder.add(
          AmbariMetricsEmitter.sanitize(
              String.valueOf(
                  serviceMetricEvent.getUserDims().get(dimName)
              )
          )
      );
    }
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

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SendAllTimelineEventConverter that = (SendAllTimelineEventConverter) o;

    if (namespacePrefix != null ? !namespacePrefix.equals(that.namespacePrefix) : that.namespacePrefix != null) {
      return false;
    }
    return appName.equals(that.appName);

  }

  @Override
  public int hashCode()
  {
    int result = namespacePrefix != null ? namespacePrefix.hashCode() : 0;
    result = 31 * result + appName.hashCode();
    return result;
  }
}
