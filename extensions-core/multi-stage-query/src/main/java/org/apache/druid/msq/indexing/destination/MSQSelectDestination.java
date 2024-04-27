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

package org.apache.druid.msq.indexing.destination;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Determines the destination for results of select queries.
 */
public enum MSQSelectDestination
{
  /**
   * Writes all the results directly to the report.
   */
  TASKREPORT("taskReport", false),
  /**
   * Writes all the results as files in a specified format to an external location outside druid.
   */
  EXPORT("export", false),
  /**
   * Writes the results as frame files to durable storage. Task report can be truncated to a preview.
   */
  DURABLESTORAGE("durableStorage", true);

  private final String name;
  private final boolean shouldTruncateResultsInTaskReport;

  public boolean shouldTruncateResultsInTaskReport()
  {
    return shouldTruncateResultsInTaskReport;
  }

  MSQSelectDestination(String name, boolean shouldTruncateResultsInTaskReport)
  {
    this.name = name;
    this.shouldTruncateResultsInTaskReport = shouldTruncateResultsInTaskReport;
  }

  @JsonValue
  public String getName()
  {
    return name;
  }

  @Override
  public String toString()
  {
    return "MSQSelectDestination{" +
           "name='" + name + '\'' +
           ", shouldTruncateResultsInTaskReport=" + shouldTruncateResultsInTaskReport +
           '}';
  }
}
