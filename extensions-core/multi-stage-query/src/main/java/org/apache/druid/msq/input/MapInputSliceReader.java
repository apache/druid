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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.counters.CounterTracker;

import java.util.Map;
import java.util.function.Consumer;

/**
 * Reader that handles multiple types of slices.
 */
public class MapInputSliceReader implements InputSliceReader
{
  private final Map<Class<? extends InputSlice>, InputSliceReader> readerMap;

  @Inject
  public MapInputSliceReader(final Map<Class<? extends InputSlice>, InputSliceReader> readerMap)
  {
    this.readerMap = ImmutableMap.copyOf(readerMap);
  }

  @Override
  public int numReadableInputs(InputSlice slice)
  {
    return getReader(slice.getClass()).numReadableInputs(slice);
  }

  @Override
  public ReadableInputs attach(
      int inputNumber,
      InputSlice slice,
      CounterTracker counters,
      Consumer<Throwable> warningPublisher
  )
  {
    return getReader(slice.getClass()).attach(inputNumber, slice, counters, warningPublisher);
  }

  private InputSliceReader getReader(final Class<? extends InputSlice> clazz)
  {
    final InputSliceReader reader = readerMap.get(clazz);

    if (reader == null) {
      throw new ISE("Cannot handle inputSpec of class [%s]", clazz.getName());
    }

    return reader;
  }
}
