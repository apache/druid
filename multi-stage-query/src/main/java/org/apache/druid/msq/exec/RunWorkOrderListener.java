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

package org.apache.druid.msq.exec;

import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;

import javax.annotation.Nullable;

/**
 * Listener for various things that may happen during execution of {@link RunWorkOrder#startAsync()}. Listener methods are
 * fired in processing threads, so they must be thread-safe, and it is important that they run quickly.
 */
public interface RunWorkOrderListener
{
  /**
   * Called when done reading input. If key statistics were gathered, they are provided.
   */
  void onDoneReadingInput(@Nullable ClusterByStatisticsSnapshot snapshot);

  /**
   * Called when an output channel becomes available for reading by downstream stages.
   */
  void onOutputChannelAvailable(OutputChannel outputChannel);

  /**
   * Called when the work order has succeeded.
   */
  void onSuccess(Object resultObject);

  /**
   * Called when a non-fatal exception is encountered. Work continues after this listener fires.
   */
  void onWarning(Throwable t);

  /**
   * Called when the work order has failed.
   */
  void onFailure(Throwable t);
}
