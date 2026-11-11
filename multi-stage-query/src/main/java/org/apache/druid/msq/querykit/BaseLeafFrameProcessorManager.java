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
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.frame.processor.manager.ProcessorAndCallback;
import org.apache.druid.frame.processor.manager.ProcessorManager;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.exec.ExecutionContext;
import org.apache.druid.msq.exec.FrameContext;
import org.apache.druid.msq.input.table.SegmentsInputSlice;
import org.apache.druid.segment.SegmentMapFunction;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Manager for processors created by {@link BaseLeafStageProcessor}.
 */
public class BaseLeafFrameProcessorManager implements ProcessorManager<Object, Long>
{
  private static final Logger log = new Logger(BaseLeafFrameProcessorManager.class);

  /**
   * Base inputs, from {@link BaseLeafStageProcessor#makeBaseInputQueue(IntList, List, ExecutionContext)}.
   */
  private final ReadableInputQueue baseInputQueue;

  /**
   * Segment map functions, one per base input, from {@link BaseLeafStageProcessor#makeSegmentMapFnProcessor}.
   */
  private final Map<Integer, SegmentMapFunction> segmentMapFns;

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

  /**
   * Set true when {@link #next()} returns its last item.
   */
  private boolean noMore;

  BaseLeafFrameProcessorManager(
      ReadableInputQueue baseInputQueue,
      List<BaseInputSegmentMapFunction> segmentMapFns,
      Queue<FrameWriterFactory> frameWriterFactoryQueue,
      Queue<WritableFrameChannel> channelQueue,
      FrameContext frameContext,
      BaseLeafStageProcessor parentFactory
  )
  {
    this.baseInputQueue = baseInputQueue;
    this.segmentMapFns =
        segmentMapFns.stream()
                     .collect(Collectors.toMap(
                         BaseInputSegmentMapFunction::inputNumber,
                         BaseInputSegmentMapFunction::segmentMapFn
                     ));
    this.frameWriterFactoryQueueRef = new AtomicReference<>(frameWriterFactoryQueue);
    this.channelQueueRef = new AtomicReference<>(channelQueue);
    this.frameContext = frameContext;
    this.parentFactory = parentFactory;
  }

  @Override
  public ListenableFuture<Optional<ProcessorAndCallback<Object>>> next()
  {
    if (noMore) {
      // Prior call would have returned empty Optional.
      throw new NoSuchElementException();
    } else {
      baseInputQueue.start();
    }

    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> nextInput = baseInputQueue.nextInput();
    if (nextInput != null) {
      return FutureUtils.transform(
          nextInput,
          numberedInput -> Optional.of(
              new ProcessorAndCallback<>(
                  withHandoffTag(
                      parentFactory.makeProcessor(
                          numberedInput.input(),
                          getSegmentMapFnForInputNumber(numberedInput.inputNumber()),
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
                      ),
                      numberedInput.inputNumber()
                  ),
                  null
              )
          )
      );
    } else {
      noMore = true;
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

    // Close baseInputQueue so all currently-loading segments are released.
    baseInputQueue.close();
  }

  private SegmentMapFunction getSegmentMapFnForInputNumber(final int inputNumber)
  {
    final SegmentMapFunction segmentMapFn = segmentMapFns.get(inputNumber);
    if (segmentMapFn == null) {
      throw DruidException.defensive("No segment map function for input number[%d]", inputNumber);
    }
    return segmentMapFn;
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

  /**
   * Wraps a leaf processor such that when it reports that it could not read some handed-off segments (by returning
   * {@link SegmentsInputSlice}), those are transformed to {@link HandedOffSegments}. This allows them to be re-read
   * with the proper {@link SegmentMapFunction} applied.
   */
  private static FrameProcessor<Object> withHandoffTag(final FrameProcessor<Object> processor, final int inputNumber)
  {
    class HandoffTaggingFrameProcessor implements FrameProcessor<Object>
    {
      @Override
      public List<ReadableFrameChannel> inputChannels()
      {
        return processor.inputChannels();
      }

      @Override
      public List<WritableFrameChannel> outputChannels()
      {
        return processor.outputChannels();
      }

      @Override
      public ReturnOrAwait<Object> runIncrementally(IntSet readableInputs) throws InterruptedException, IOException
      {
        final ReturnOrAwait<Object> retVal = processor.runIncrementally(readableInputs);

        if (retVal.isReturn() && retVal.value() instanceof SegmentsInputSlice slice) {
          return ReturnOrAwait.returnObject(new HandedOffSegments(inputNumber, slice));
        } else {
          return retVal;
        }
      }

      @Override
      public void cleanup() throws IOException
      {
        processor.cleanup();
      }
    }

    return new HandoffTaggingFrameProcessor();
  }

  /**
   * Segments handed off by a realtime server while a processor was reading from it, along with the input number of
   * the slice that server belonged to.
   */
  public record HandedOffSegments(int inputNumber, SegmentsInputSlice slice)
  {
  }
}
