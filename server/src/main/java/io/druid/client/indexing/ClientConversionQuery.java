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

package io.druid.client.indexing;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

/**
 */
public class ClientConversionQuery
{
  private final String dataSource;
  private final Interval interval;
  private final DataSegment segment;

  public ClientConversionQuery(
      DataSegment segment
  )
  {
    this.dataSource = segment.getDataSource();
    this.interval = segment.getInterval();
    this.segment = segment;
  }

  public ClientConversionQuery(
      String dataSource,
      Interval interval
  )
  {
    this.dataSource = dataSource;
    this.interval = interval;
    this.segment = null;
  }

  @JsonProperty
  public String getType()
  {
    return "version_converter";
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  public DataSegment getSegment()
  {
    return segment;
  }
}
