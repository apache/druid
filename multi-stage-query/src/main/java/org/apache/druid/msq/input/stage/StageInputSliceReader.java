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

package org.apache.druid.msq.input.stage;

import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.input.PhysicalInputSlice;

import java.util.Collections;
import java.util.function.Consumer;

/**
 * Reads slices of type {@link StageInputSlice}.
 */
public class StageInputSliceReader implements InputSliceReader
{
  public static final StageInputSliceReader INSTANCE = new StageInputSliceReader();

  private StageInputSliceReader()
  {
    // Singleton.
  }

  @Override
  public PhysicalInputSlice attach(
      final int inputNumber,
      final InputSlice slice,
      final CounterTracker counters,
      final Consumer<Throwable> warningPublisher
  )
  {
    final StageInputSlice stageInputSlice = (StageInputSlice) slice;
    return new PhysicalInputSlice(stageInputSlice.getPartitions(), Collections.emptyList(), Collections.emptyList());
  }
}
