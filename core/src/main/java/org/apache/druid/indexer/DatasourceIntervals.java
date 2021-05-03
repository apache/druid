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

package org.apache.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import org.joda.time.Interval;

import java.util.List;

/**
 * Contains a List of Intervals for a datasource.
 */
public class DatasourceIntervals
{
  private final String datasource;
  private final List<Interval> intervals;

  @JsonCreator
  public DatasourceIntervals(
      @JsonProperty("datasource") String datasource,
      @JsonProperty("intervals") List<Interval> intervals
  )
  {
    this.datasource = datasource;
    this.intervals = intervals;
  }

  @JsonProperty("datasource")
  public String getDatasource()
  {
    return datasource;
  }

  @JsonProperty("intervals")
  public List<Interval> getIntervals()
  {
    return intervals;
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                  .add("datasource", datasource)
                  .add("intervals", intervals)
                  .toString();
  }
}
