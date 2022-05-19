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

package org.apache.druid.java.util.common.guava;

import java.io.IOException;

/**
 * A Sequence that is based entirely on the Yielder implementation.
 * <p/>
 * This is a base class to simplify the creation of Sequences.
 */
public abstract class YieldingSequenceBase<T> implements Sequence<T>
{
  @Override
  public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
  {
    final OutType retVal;
    Yielder<OutType> yielder = toYielder(initValue, YieldingAccumulators.fromAccumulator(accumulator));

    try {
      while (!yielder.isDone()) {
        yielder = yielder.next(yielder.get());
      }
      retVal = yielder.get();
    }
    catch (Throwable t1) {
      try {
        yielder.close();
      }
      catch (Throwable t2) {
        t1.addSuppressed(t2);
      }

      throw t1;
    }

    try {
      yielder.close();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    return retVal;
  }
}
