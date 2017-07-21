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

package io.druid.emitter.graphite;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.metamx.emitter.service.ServiceMetricEvent;

import java.util.concurrent.TimeUnit;

/**
 * Emits all the events instance of {@link ServiceMetricEvent}.
 * <p>
 * All the dimensions will be retained and lexicographically order using dimensions name.
 * <p>
 * The metric path of the graphite event is:
 * <namespacePrefix>.[<druid service name>].[<druid hostname>].<dimensions values ordered by dimension's name>.<metric>
 * <p>
 * Note that this path will be sanitized by replacing all the `.` or `space` to `_` {@link GraphiteEmitter#sanitize(String)}
 */

@JsonTypeName("all")
public class SendAllGraphiteEventConverter implements DruidToGraphiteEventConverter
{
  @JsonProperty
  private final boolean ignoreHostname;

  @JsonProperty
  private final boolean ignoreServiceName;

  @JsonProperty
  private final String namespacePrefix;

  @JsonProperty
  public String getNamespacePrefix()
  {
    return namespacePrefix;
  }

  @JsonProperty
  public boolean isIgnoreServiceName()
  {
    return ignoreServiceName;
  }

  @JsonProperty
  public boolean isIgnoreHostname()
  {
    return ignoreHostname;
  }

  @JsonCreator
  public SendAllGraphiteEventConverter(
      @JsonProperty("namespacePrefix") String namespacePrefix,
      @JsonProperty("ignoreHostname") Boolean ignoreHostname,
      @JsonProperty("ignoreServiceName") Boolean ignoreServiceName
  )
  {
    this.ignoreHostname = ignoreHostname == null ? false : ignoreHostname;
    this.ignoreServiceName = ignoreServiceName == null ? false : ignoreServiceName;
    this.namespacePrefix = Preconditions.checkNotNull(namespacePrefix, "namespace prefix can not be null");
  }

  @Override
  public GraphiteEvent druidEventToGraphite(ServiceMetricEvent serviceMetricEvent)
  {
    ImmutableList.Builder metricPathBuilder = new ImmutableList.Builder<String>();
    metricPathBuilder.add(this.getNamespacePrefix());
    if (!this.isIgnoreServiceName()) {
      metricPathBuilder.add(GraphiteEmitter.sanitize(serviceMetricEvent.getService()));
    }
    if (!this.isIgnoreHostname()) {
      metricPathBuilder.add(GraphiteEmitter.sanitize(serviceMetricEvent.getHost()));
    }

    ImmutableSortedSet<String> dimNames = ImmutableSortedSet.copyOf(serviceMetricEvent.getUserDims().keySet());
    for (String dimName : dimNames) {
      metricPathBuilder.add(GraphiteEmitter.sanitize(String.valueOf(serviceMetricEvent.getUserDims()
                                                                                      .get(dimName))));
    }
    metricPathBuilder.add(GraphiteEmitter.sanitize(serviceMetricEvent.getMetric()));

    return new GraphiteEvent(
        Joiner.on(".").join(metricPathBuilder.build()),
        serviceMetricEvent.getValue().toString(),
        TimeUnit.MILLISECONDS.toSeconds(serviceMetricEvent.getCreatedTime().getMillis())
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SendAllGraphiteEventConverter)) {
      return false;
    }

    SendAllGraphiteEventConverter that = (SendAllGraphiteEventConverter) o;

    if (isIgnoreHostname() != that.isIgnoreHostname()) {
      return false;
    }
    if (isIgnoreServiceName() != that.isIgnoreServiceName()) {
      return false;
    }
    return getNamespacePrefix().equals(that.getNamespacePrefix());

  }

  @Override
  public int hashCode()
  {
    int result = (isIgnoreHostname() ? 1 : 0);
    result = 31 * result + (isIgnoreServiceName() ? 1 : 0);
    result = 31 * result + getNamespacePrefix().hashCode();
    return result;
  }
}
