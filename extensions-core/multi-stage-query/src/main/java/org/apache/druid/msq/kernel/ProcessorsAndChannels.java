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

import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.OutputChannels;
import org.apache.druid.java.util.common.guava.Sequence;

/**
 * Returned from {@link FrameProcessorFactory#makeProcessors}.
 *
 * Includes a processor sequence and a list of output channels.
 */
public class ProcessorsAndChannels<ProcessorClass extends FrameProcessor<T>, T>
{
  private final Sequence<ProcessorClass> workers;
  private final OutputChannels outputChannels;

  public ProcessorsAndChannels(
      final Sequence<ProcessorClass> workers,
      final OutputChannels outputChannels
  )
  {
    this.workers = workers;
    this.outputChannels = outputChannels;
  }

  public Sequence<ProcessorClass> processors()
  {
    return workers;
  }

  public OutputChannels getOutputChannels()
  {
    return outputChannels;
  }
}
