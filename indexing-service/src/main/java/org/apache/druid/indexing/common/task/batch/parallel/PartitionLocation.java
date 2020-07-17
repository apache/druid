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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.StringUtils;
import org.joda.time.Interval;

import java.net.URI;
import java.util.Objects;

/**
 * This class represents the intermediary data server where the partition of {@link #interval} and
 * {@link #getBucketId()} is stored.
 */
abstract class PartitionLocation<T>
{
  private final String host;
  private final int port;
  private final boolean useHttps;
  private final String subTaskId;
  private final Interval interval;
  private final T secondaryPartition;

  PartitionLocation(
      String host,
      int port,
      boolean useHttps,
      String subTaskId,
      Interval interval,
      T secondaryPartition
  )
  {
    this.host = host;
    this.port = port;
    this.useHttps = useHttps;
    this.subTaskId = subTaskId;
    this.interval = interval;
    this.secondaryPartition = secondaryPartition;
  }

  @JsonProperty
  public String getHost()
  {
    return host;
  }

  @JsonProperty
  public int getPort()
  {
    return port;
  }

  @JsonProperty
  public boolean isUseHttps()
  {
    return useHttps;
  }

  @JsonProperty
  public String getSubTaskId()
  {
    return subTaskId;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @JsonIgnore
  public T getSecondaryPartition()
  {
    return secondaryPartition;
  }

  abstract int getBucketId();

  final URI toIntermediaryDataServerURI(String supervisorTaskId)
  {
    return URI.create(
        StringUtils.format(
            "%s://%s:%d/druid/worker/v1/shuffle/task/%s/%s/partition?startTime=%s&endTime=%s&bucketId=%d",
            useHttps ? "https" : "http",
            host,
            port,
            StringUtils.urlEncode(supervisorTaskId),
            StringUtils.urlEncode(subTaskId),
            interval.getStart(),
            interval.getEnd(),
            getBucketId()
        )
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
    PartitionLocation<?> that = (PartitionLocation<?>) o;
    return port == that.port &&
           useHttps == that.useHttps &&
           Objects.equals(host, that.host) &&
           Objects.equals(subTaskId, that.subTaskId) &&
           Objects.equals(interval, that.interval) &&
           Objects.equals(secondaryPartition, that.secondaryPartition);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(host, port, useHttps, subTaskId, interval, secondaryPartition);
  }

  @Override
  public String toString()
  {
    return "PartitionLocation{" +
           "host='" + host + '\'' +
           ", port=" + port +
           ", useHttps=" + useHttps +
           ", subTaskId='" + subTaskId + '\'' +
           ", interval=" + interval +
           ", secondaryPartition=" + secondaryPartition +
           '}';
  }
}
