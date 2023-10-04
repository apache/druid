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

package org.apache.druid.frame.processor.manager;

import com.google.common.base.Preconditions;
import org.apache.druid.frame.processor.FrameProcessor;

import javax.annotation.Nullable;
import java.util.function.Consumer;

/**
 * Processor and success callback returned by {@link ProcessorManager#next()}.
 */
public class ProcessorAndCallback<T>
{
  private final FrameProcessor<T> processor;
  private final Consumer<T> callback;

  public ProcessorAndCallback(FrameProcessor<T> processor, @Nullable Consumer<T> callback)
  {
    this.processor = Preconditions.checkNotNull(processor, "processor");
    this.callback = callback;
  }

  public FrameProcessor<T> processor()
  {
    return processor;
  }

  public void onComplete(final T resultObject)
  {
    if (callback != null) {
      callback.accept(resultObject);
    }
  }
}
