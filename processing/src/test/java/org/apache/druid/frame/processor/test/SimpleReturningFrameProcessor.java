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

package org.apache.druid.frame.processor.test;

import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.processor.FrameProcessor;

/**
 * A {@link FrameProcessor} which doesn't write anything to channels.
 * Instead, it finishes on the first call to {@link #runIncrementally(IntSet)}, and returns the set
 * {@link #returnValue}.
 */
public class SimpleReturningFrameProcessor<T> extends SingleChannelFrameProcessor<T>
{
  private final T returnValue;

  public SimpleReturningFrameProcessor(T returnValue)
  {
    super(null, null);
    this.returnValue = returnValue;
  }

  @Override
  public T doSimpleWork()
  {
    return returnValue;
  }
}
