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

package org.apache.druid.msq.util;

import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.table.TableInputSpec;
import org.apache.druid.msq.kernel.StageDefinition;
import org.joda.time.Interval;

import java.util.HashSet;
import java.util.Set;

/**
 * Utility methods for setting up dimensions on metrics.
 */
public class MSQMetricUtils
{
  /**
   * Returns a set of all intervals of all {@link TableInputSpec} in the stageDefinition.
   */
  public static Set<Interval> getIntervals(final StageDefinition stageDefinition)
  {
    HashSet<Interval> intervals = new HashSet<>();
    for (InputSpec inputSpec : stageDefinition.getInputSpecs()) {
      if (inputSpec instanceof TableInputSpec) {
        TableInputSpec tableInputSpec = (TableInputSpec) inputSpec;
        intervals.addAll(tableInputSpec.getIntervals());
      }
    }
    return intervals;
  }
}
