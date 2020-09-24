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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.datasketches.hll.HllSketch;
import org.joda.time.Interval;

import java.util.Map;
import java.util.Objects;

public class DimensionCardinalityReport implements SubTaskReport
{
  // We choose logK=11 because the following link shows that HllSketch with K=2048 has roughly the same
  // serialized size as HyperLogLogCollector.
  // http://datasketches.apache.org/docs/HLL/HllSketchVsDruidHyperLogLogCollector.html
  public static final int HLL_SKETCH_LOG_K = 11;

  static final String TYPE = "dimension_cardinality";
  private static final String PROP_CARDINALITIES = "cardinalities";


  private final String taskId;

  /**
   * A map of intervals to byte arrays, representing {@link HllSketch} objects,
   * serialized using {@link HllSketch#toCompactByteArray()}.
   *
   * The HllSketch objects should be created with the HLL_SKETCH_LOG_K constant defined in this class.
   *
   * The collector is used to determine cardinality estimates for each interval.
   */
  private final Map<Interval, byte[]> intervalToCardinalities;

  @JsonCreator
  public DimensionCardinalityReport(
      @JsonProperty("taskId") String taskId,
      @JsonProperty(PROP_CARDINALITIES) Map<Interval, byte[]> intervalToCardinalities
  )
  {
    this.taskId = taskId;
    this.intervalToCardinalities = intervalToCardinalities;
  }

  @Override
  @JsonProperty
  public String getTaskId()
  {
    return taskId;
  }

  @JsonProperty(PROP_CARDINALITIES)
  public Map<Interval, byte[]> getIntervalToCardinalities()
  {
    return intervalToCardinalities;
  }

  @Override
  public String toString()
  {
    return "DimensionCardinalityReport{" +
           "taskId='" + taskId + '\'' +
           ", intervalToCardinalities=" + intervalToCardinalities +
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
    DimensionCardinalityReport that = (DimensionCardinalityReport) o;
    return Objects.equals(getTaskId(), that.getTaskId()) &&
           Objects.equals(getIntervalToCardinalities(), that.getIntervalToCardinalities());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getTaskId(), getIntervalToCardinalities());
  }

  public static HllSketch createHllSketchForReport()
  {
    return new HllSketch(HLL_SKETCH_LOG_K);
  }
}
