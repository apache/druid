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

/**
 * Represents the current progress of {@link SinglePhaseParallelIndexTaskRunner}.
 */
class SinglePhaseParallelIndexingProgress implements ParallelIndexingProgress
{
  /**
   * Number of running tasks.
   */
  private final int running;

  /**
   * Number of succeeded tasks.
   */
  private final int succeeded;

  /**
   * Number of failed tasks.
   */
  private final int failed;

  /**
   * Number of complete (succeeded + failed) tasks.
   */
  private final int complete;

  /**
   * Number of total (running + complete) tasks.
   */
  private final int total;

  /**
   * Number of succeeded tasks for {@link SinglePhaseParallelIndexTaskRunner} to succeed.
   */
  private final int expectedSucceeded;

  static SinglePhaseParallelIndexingProgress notRunning()
  {
    return new SinglePhaseParallelIndexingProgress(0, 0, 0, 0, 0, -1);
  }

  @JsonCreator
  SinglePhaseParallelIndexingProgress(
      @JsonProperty("running") int running,
      @JsonProperty("succeeded") int succeeded,
      @JsonProperty("failed") int failed,
      @JsonProperty("complete") int complete,
      @JsonProperty("total") int total,
      @JsonProperty("expectedSucceeded") int expectedSucceeded
  )
  {
    this.running = running;
    this.succeeded = succeeded;
    this.failed = failed;
    this.complete = complete;
    this.total = total;
    this.expectedSucceeded = expectedSucceeded;
  }

  @JsonProperty
  public int getRunning()
  {
    return running;
  }

  @JsonProperty
  public int getSucceeded()
  {
    return succeeded;
  }

  @JsonProperty
  public int getFailed()
  {
    return failed;
  }

  @JsonProperty
  public int getComplete()
  {
    return complete;
  }

  @JsonProperty
  public int getTotal()
  {
    return total;
  }

  @JsonProperty
  public int getExpectedSucceeded()
  {
    return expectedSucceeded;
  }
}
