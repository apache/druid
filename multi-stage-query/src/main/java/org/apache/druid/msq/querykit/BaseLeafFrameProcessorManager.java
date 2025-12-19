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

package org.apache.druid.msq.querykit;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.manager.ProcessorAndCallback;
import org.apache.druid.frame.processor.manager.ProcessorManager;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.exec.FrameContext;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.segment.SegmentMapFunction;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Manager for processors created by {@link BaseLeafStageProcessor}.
 */
public class BaseLeafFrameProcessorManager implements ProcessorManager<Object, Long>
{
  private static final Logger log = new Logger(BaseLeafFrameProcessorManager.class);

  /**
   * Base inputs, from {@link BaseLeafStageProcessor#readBaseInputs}. Set to null by {@link #next()}
   * once exhausted.
   */
  @Nullable
  private Iterator<ReadableInput> baseInputIterator;

  /**
   * Segment map function for this processor, from {@link BaseLeafStageProcessor#makeSegmentMapFnProcessor}.
   */
  private final SegmentMapFunction segmentMapFn;

  /**
   * Frame writer factories.
   *
   * Sychronized by itself. Not marked with {@link com.google.errorprone.annotations.concurrent.GuardedBy} because
   * errorprone has difficulty tracking synchronization through {@link #makeLazyResourceHolder}.
   */
  private final AtomicReference<Queue<FrameWriterFactory>> frameWriterFactoryQueueRef;

  /**
   * Output channels.
   *
   * Sychronized by itself. Not marked with {@link com.google.errorprone.annotations.concurrent.GuardedBy} because
   * errorprone has difficulty tracking synchronization through {@link #makeLazyResourceHolder}.
   */
  private final AtomicReference<Queue<WritableFrameChannel>> channelQueueRef;

  /**
   * Frame context from our parent.
   */
  private final FrameContext frameContext;

  /**
   * Parent, used for {@link BaseLeafStageProcessor#makeProcessor}.
   */
  private final BaseLeafStageProcessor parentFactory;

  BaseLeafFrameProcessorManager(
      Iterable<ReadableInput> baseInputs,
      SegmentMapFunction segmentMapFn,
      Queue<FrameWriterFactory> frameWriterFactoryQueue,
      Queue<WritableFrameChannel> channelQueue,
      FrameContext frameContext,
      BaseLeafStageProcessor parentFactory
  )
  {
    this.baseInputIterator = baseInputs.iterator();
    this.segmentMapFn = segmentMapFn;
    this.frameWriterFactoryQueueRef = new AtomicReference<>(frameWriterFactoryQueue);
    this.channelQueueRef = new AtomicReference<>(channelQueue);
    this.frameContext = frameContext;
    this.parentFactory = parentFactory;
  }

  @Override
  public ListenableFuture<Optional<ProcessorAndCallback<Object>>> next()
  {
    if (baseInputIterator == null) {
      // Prior call would have returned empty Optional.
      throw new NoSuchElementException();
    } else if (baseInputIterator.hasNext()) {
      final ReadableInput baseInput = baseInputIterator.next();
      final FrameProcessor<Object> processor = parentFactory.makeProcessor(
          baseInput,
          segmentMapFn,
          makeLazyResourceHolder(
              channelQueueRef,
              channel -> {
                try {
                  channel.close();
                }
                catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
          ),
          makeLazyResourceHolder(frameWriterFactoryQueueRef, ignored -> {}),
          frameContext
      );

      return Futures.immediateFuture(Optional.of(new ProcessorAndCallback<>(processor, null)));
    } else {
      baseInputIterator = null;
      return Futures.immediateFuture(Optional.empty());
    }
  }

  @Override
  public Long result()
  {
    // Return value isn't used for anything. Must be a Long for backwards-compatibility.
    return 0L;
  }

  @Override
  public void close()
  {
    final Queue<WritableFrameChannel> channelQueue;
    synchronized (channelQueueRef) {
      // Set to null so any channels returned by outstanding workers are immediately closed.
      channelQueue = channelQueueRef.getAndSet(null);
    }

    WritableFrameChannel c;
    while ((c = channelQueue.poll()) != null) {
      try {
        c.close();
      }
      catch (Throwable e) {
        log.warn(e, "Error encountered while closing channel for [%s]", this);
      }
    }
  }

  private static <T> ResourceHolder<T> makeLazyResourceHolder(
      final AtomicReference<Queue<T>> queueRef,
      final Consumer<T> backupCloser
  )
  {
    return new LazyResourceHolder<>(
        () -> {
          final T resource;

          synchronized (queueRef) {
            resource = queueRef.get().poll();
          }

          return new ResourceHolder<T>()
          {
            @Override
            public T get()
            {
              return resource;
            }

            @Override
            public void close()
            {
              synchronized (queueRef) {
                final Queue<T> queue = queueRef.get();
                if (queue != null) {
                  queue.add(resource);
                  return;
                }
              }

              // Queue was null
              backupCloser.accept(resource);
            }
          };
        }
    );
  }
}
