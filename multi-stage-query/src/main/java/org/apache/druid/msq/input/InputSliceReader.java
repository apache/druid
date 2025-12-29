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

package org.apache.druid.msq.input;

import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.exec.DataServerQueryHandler;
import org.apache.druid.msq.input.stage.ReadablePartition;
import org.apache.druid.msq.input.stage.ReadablePartitions;
import org.apache.druid.msq.input.stage.StageInputSlice;

import java.util.function.Consumer;

/**
 * Reads {@link InputSlice} on workers.
 */
public interface InputSliceReader
{
  /**
   * Prepares an input slice for reading. Does not actually begin reading. The returned {@link PhysicalInputSlice}
   * contains {@link ReadablePartitions} from {@link StageInputSlice} and pointers to {@link LoadableSegment}
   * or {@link DataServerQueryHandler} for all other slice types.
   *
   * @throws UnsupportedOperationException if this reader does not support this spec
   */
  PhysicalInputSlice attach(
      int inputNumber,
      InputSlice slice,
      CounterTracker counters,
      Consumer<Throwable> warningPublisher
  );
}
