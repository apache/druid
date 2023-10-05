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

package org.apache.druid.msq.kernel;

import org.apache.druid.frame.processor.OutputChannels;
import org.apache.druid.frame.processor.manager.ProcessorManager;

/**
 * Returned from {@link FrameProcessorFactory#makeProcessors}.
 *
 * Includes a processor sequence and a list of output channels.
 *
 * @param <T> return type of {@link org.apache.druid.frame.processor.FrameProcessor} from {@link #getProcessorManager()}
 * @param <R> result type of {@link ProcessorManager#result()}
 */
public class ProcessorsAndChannels<T, R>
{
  private final ProcessorManager<T, R> processorManager;
  private final OutputChannels outputChannels;

  public ProcessorsAndChannels(
      final ProcessorManager<T, R> processorManager,
      final OutputChannels outputChannels
  )
  {
    this.processorManager = processorManager;
    this.outputChannels = outputChannels;
  }

  public ProcessorManager<T, R> getProcessorManager()
  {
    return processorManager;
  }

  public OutputChannels getOutputChannels()
  {
    return outputChannels;
  }
}
