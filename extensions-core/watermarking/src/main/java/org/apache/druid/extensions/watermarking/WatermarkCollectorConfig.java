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

package org.apache.druid.extensions.watermarking;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.extensions.watermarking.gaps.GapDetectorFactory;
import org.apache.druid.extensions.watermarking.watermarks.BatchCompletenessLowWatermark;
import org.apache.druid.extensions.watermarking.watermarks.WatermarkCursorFactory;

import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;

public class WatermarkCollectorConfig extends WatermarkKeeperConfig
{
  @JsonProperty("numThreads")
  private int numThreads = 16;

  @JsonProperty("traceAnchorWatermark")
  private String traceAnchorWatermark = BatchCompletenessLowWatermark.TYPE;

  @JsonProperty("cursors")
  @NotNull
  private List<Class<? extends WatermarkCursorFactory>> cursors = Collections.emptyList();

  @JsonProperty("gapDetectors")
  @NotNull
  private List<Class<? extends GapDetectorFactory>> gapDetectors = Collections.emptyList();

  @JsonProperty("reconcileAtStartup")
  private Boolean reconcileAtStartup = true;

  public int getNumThreads()
  {
    return numThreads;
  }

  public String getAnchorWatermark()
  {
    return traceAnchorWatermark;
  }

  public Boolean getReconcileAtStartup()
  {
    return reconcileAtStartup;
  }

  public List<Class<? extends WatermarkCursorFactory>> getCursors()
  {
    return cursors != null && cursors.size() > 0
           ? cursors
           : Collections.emptyList();
  }

  public List<Class<? extends GapDetectorFactory>> getGapDetectors()
  {
    return gapDetectors != null && gapDetectors.size() > 0
           ? gapDetectors
           : Collections.emptyList();
  }
}
