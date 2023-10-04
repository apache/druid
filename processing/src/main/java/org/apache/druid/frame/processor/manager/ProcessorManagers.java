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

import com.google.common.collect.Iterators;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;

import java.util.function.Supplier;

/**
 * Utility functions for creating {@link ProcessorManager}.
 */
public class ProcessorManagers
{
  private ProcessorManagers()
  {
    // No instantiation.
  }

  /**
   * Manager with zero processors. Returns the number of processors run.
   */
  public static <T> ProcessorManager<T, Long> none()
  {
    return of(Sequences.empty());
  }

  /**
   * Manager with processors derived from a {@link Sequence}. Returns the number of processors run.
   */
  public static <T> ProcessorManager<T, Long> of(final Sequence<? extends FrameProcessor<T>> processors)
  {
    return new SequenceProcessorManager<>(processors);
  }

  /**
   * Manager with processors derived from an {@link Iterable}. Returns the number of processors run.
   */
  public static <T> ProcessorManager<T, Long> of(final Iterable<? extends FrameProcessor<T>> processors)
  {
    return of(Sequences.simple(processors));
  }

  /**
   * Manager with a single processor derived from a {@link Supplier}. Returns the number of processors run.
   */
  public static <T> ProcessorManager<T, Long> of(final Supplier<? extends FrameProcessor<T>> processors)
  {
    return of(Sequences.simple(() -> Iterators.singletonIterator(processors.get())));
  }
}
