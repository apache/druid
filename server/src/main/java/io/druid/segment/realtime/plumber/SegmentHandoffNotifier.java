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

package io.druid.segment.realtime.plumber;

import io.druid.query.SegmentDescriptor;

import java.io.Closeable;
import java.util.concurrent.Executor;

public interface SegmentHandoffNotifier extends Closeable
{
  /**
   * register a handOffCallback to be called when segment handoff is complete.
   *
   * @param descriptor      segment descriptor for the segment for which handoffCallback is requested
   * @param exec            executor used to call the runnable
   * @param handOffRunnable runnable to be called when segment handoff is complete
   */
  boolean registerSegmentHandoffCallback(
      SegmentDescriptor descriptor,
      Executor exec,
      Runnable handOffRunnable
  );

  /**
   * Perform any initial setup. Should be called before using any other methods, and should be paired
   * with a corresponding call to {@link #stop()}.
   */
  void start();

  /**
   * Perform any final processing and clean up after ourselves.
   */
  @Override
  void close();

}
