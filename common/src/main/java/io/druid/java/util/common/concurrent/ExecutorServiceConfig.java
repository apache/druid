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

package io.druid.java.util.common.concurrent;

import org.skife.config.Config;
import org.skife.config.Default;

/**
 */
public abstract class ExecutorServiceConfig
{
  public static final int DEFAULT_NUM_THREADS = -1;

  @Config(value = "${base_path}.formatString")
  @Default("processing-%s")
  public abstract String getFormatString();

  public int getNumThreads()
  {
    int numThreadsConfigured = getNumThreadsConfigured();
    if (numThreadsConfigured != DEFAULT_NUM_THREADS) {
      return numThreadsConfigured;
    } else {
      return Math.max(Runtime.getRuntime().availableProcessors() - 1, 1);
    }
  }

  /**
   * Returns the number of threads _explicitly_ configured, or -1 if it is not explicitly configured, that is not
   * a valid number of threads. To get the configured value or the default (valid) number, use {@link #getNumThreads()}.
   * This method exists for ability to distinguish between the default value set when there is no explicit config, and
   * an explicitly configured value.
   */
  @Config(value = "${base_path}.numThreads")
  public int getNumThreadsConfigured()
  {
    return DEFAULT_NUM_THREADS;
  }
}
