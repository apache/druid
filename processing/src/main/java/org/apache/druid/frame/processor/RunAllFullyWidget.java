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

package org.apache.druid.frame.processor;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

/**
 * Helper for {@link FrameProcessorExecutor#runAllFully}. See the javadoc for that method for details about the
 * expected behavior of this class.
 *
 * One instance of this class is created for each call to {@link FrameProcessorExecutor#runAllFully}. It gradually
 * executes all processors from the provided sequence using the {@link FrameProcessorExecutor#runFully} method.
 * The {@code bouncer} and {@code maxOutstandingProcessors} parameters are used to control how many processors are
 * executed on the {@link FrameProcessorExecutor} concurrently.
 */
public class RunAllFullyWidget<T, ResultType>
{
  private static final Logger log = new Logger(RunAllFullyWidget.class);

  private final Sequence<? extends FrameProcessor<T>> processors;
  private final FrameProcessorExecutor exec;
  private final ResultType initialResult;
  private final BiFunction<ResultType, T, ResultType> accumulateFn;
  private final int maxOutstandingProcessors;
  private final Bouncer bouncer;
  @Nullable
  private final String cancellationId;

  RunAllFullyWidget(
      Sequence<? extends FrameProcessor<T>> processors,
      FrameProcessorExecutor exec,
      ResultType initialResult,
      BiFunction<ResultType, T, ResultType> accumulateFn,
      int maxOutstandingProcessors,
      Bouncer bouncer,
      @Nullable String cancellationId
  )
  {
    this.processors = processors;
    this.exec = exec;
    this.initialResult = initialResult;
    this.accumulateFn = accumulateFn;
    this.maxOutstandingProcessors = maxOutstandingProcessors;
    this.bouncer = bouncer;
    this.cancellationId = cancellationId;
  }

  ListenableFuture<ResultType> run()
  {
    final Yielder<? extends FrameProcessor<T>> processorYielder;

    try {
      processorYielder = Yielders.each(processors);
    }
    catch (Throwable e) {
      return Futures.immediateFailedFuture(e);
    }

    if (processorYielder.isDone()) {
      return Futures.immediateFuture(initialResult);
    }

    // This single RunAllFullyRunnable will be submitted to the executor "maxOutstandingProcessors" times.
    final RunAllFullyRunnable runnable = new RunAllFullyRunnable(processorYielder);

    for (int i = 0; i < maxOutstandingProcessors; i++) {
      exec.getExecutorService().submit(runnable);
    }

    return runnable.finishedFuture;
  }

  private class RunAllFullyRunnable implements Runnable
  {
    private final AtomicReference<Either<Throwable, ResultType>> finished = new AtomicReference<>();
    private final SettableFuture<ResultType> finishedFuture;
    private final Object runAllFullyLock = new Object();

    @GuardedBy("runAllFullyLock")
    Yielder<? extends FrameProcessor<T>> processorYielder;

    @GuardedBy("runAllFullyLock")
    ResultType currentResult = null;

    @GuardedBy("runAllFullyLock")
    boolean seenFirstResult = false;

    @GuardedBy("runAllFullyLock")
    int outstandingProcessors = 0;

    @GuardedBy("runAllFullyLock")
    Set<ListenableFuture<?>> outstandingFutures = Collections.newSetFromMap(new IdentityHashMap<>());

    @Nullable // nulled out by cleanup()
    @GuardedBy("runAllFullyLock")
    Queue<Bouncer.Ticket> bouncerQueue = new ArrayDeque<>();

    private RunAllFullyRunnable(final Yielder<? extends FrameProcessor<T>> processorYielder)
    {
      this.processorYielder = processorYielder;
      this.finishedFuture = exec.registerCancelableFuture(SettableFuture.create(), false, cancellationId);
      this.finishedFuture.addListener(
          () -> {
            if (finishedFuture.isCancelled()) {
              try {
                synchronized (runAllFullyLock) {
                  // Cancel any outstanding processors.
                  ImmutableList.copyOf(outstandingFutures).forEach(f -> f.cancel(true));

                  // Cleanup if there were no outstanding processors. (If there were some outstanding ones, future
                  // cancellation above would trigger cleanup.)
                  cleanupIfNoMoreProcessors();
                }
              }
              catch (Throwable e) {
                // No point throwing. Exceptions thrown in listeners don't go anywhere.
                log.warn(e, "Exception encountered while cleaning up canceled runAllFully execution");
              }
            }
          },
          Execs.directExecutor()
      );
    }

    @Override
    public void run()
    {
      final FrameProcessor<T> nextProcessor;
      Bouncer.Ticket nextTicket = null;

      synchronized (runAllFullyLock) {
        if (finished.get() != null) {
          cleanupIfNoMoreProcessors();
          return;
        } else if (!processorYielder.isDone()) {
          assert bouncerQueue != null;

          try {
            final Bouncer.Ticket ticketFromQueue = bouncerQueue.poll();

            if (ticketFromQueue != null) {
              nextTicket = ticketFromQueue;
            } else {
              final ListenableFuture<Bouncer.Ticket> ticketFuture =
                  exec.registerCancelableFuture(bouncer.ticket(), false, cancellationId);

              if (ticketFuture.isDone() && !ticketFuture.isCancelled()) {
                nextTicket = FutureUtils.getUncheckedImmediately(ticketFuture);
              } else {
                ticketFuture.addListener(
                    () -> {
                      if (!ticketFuture.isCancelled()) {
                        // No need to check for exception; bouncer tickets cannot have exceptions.
                        final Bouncer.Ticket ticket = FutureUtils.getUncheckedImmediately(ticketFuture);

                        synchronized (runAllFullyLock) {
                          if (finished.get() != null) {
                            ticket.giveBack();
                            return;
                          } else {
                            bouncerQueue.add(ticket);
                          }
                        }
                        exec.getExecutorService().submit(RunAllFullyRunnable.this);
                      }
                    },
                    Execs.directExecutor()
                );

                return;
              }
            }

            assert outstandingProcessors < maxOutstandingProcessors;
            nextProcessor = processorYielder.get();
            processorYielder = processorYielder.next(null);
            outstandingProcessors++;
          }
          catch (Throwable e) {
            if (nextTicket != null) {
              nextTicket.giveBack();
            }
            finished.compareAndSet(null, Either.error(e));
            cleanupIfNoMoreProcessors();
            return;
          }
        } else {
          return;
        }

        assert nextTicket != null;
        final ListenableFuture<T> future = exec.runFully(
            FrameProcessors.withBaggage(nextProcessor, nextTicket::giveBack),
            cancellationId
        );

        outstandingFutures.add(future);

        Futures.addCallback(
            future,
            new FutureCallback<T>()
            {
              @Override
              public void onSuccess(T result)
              {
                final boolean isDone;
                ResultType retVal = null;

                try {
                  synchronized (runAllFullyLock) {
                    outstandingProcessors--;
                    outstandingFutures.remove(future);

                    if (!seenFirstResult) {
                      currentResult = accumulateFn.apply(initialResult, result);
                      seenFirstResult = true;
                    } else {
                      currentResult = accumulateFn.apply(currentResult, result);
                    }

                    isDone = outstandingProcessors == 0 && processorYielder.isDone();

                    if (isDone) {
                      retVal = currentResult;
                    }
                  }
                }
                catch (Throwable e) {
                  finished.compareAndSet(null, Either.error(e));

                  synchronized (runAllFullyLock) {
                    cleanupIfNoMoreProcessors();
                  }

                  return;
                }

                if (isDone) {
                  finished.compareAndSet(null, Either.value(retVal));

                  synchronized (runAllFullyLock) {
                    cleanupIfNoMoreProcessors();
                  }
                } else {
                  // Not finished; run again.
                  exec.getExecutorService().submit(RunAllFullyRunnable.this);
                }
              }

              @Override
              public void onFailure(Throwable t)
              {
                finished.compareAndSet(null, Either.error(t));

                synchronized (runAllFullyLock) {
                  outstandingProcessors--;
                  outstandingFutures.remove(future);
                  cleanupIfNoMoreProcessors();
                }
              }
            }
        );
      }
    }

    /**
     * Run {@link #cleanup()} if no more processors will be launched.
     */
    @GuardedBy("runAllFullyLock")
    private void cleanupIfNoMoreProcessors()
    {
      if (outstandingProcessors == 0 && finished.get() != null) {
        cleanup();
      }
    }

    /**
     * Cleanup work that must happen after everything has calmed down.
     */
    @GuardedBy("runAllFullyLock")
    private void cleanup()
    {
      assert finished.get() != null;
      assert outstandingProcessors == 0;

      try {
        if (bouncerQueue != null) {
          // Drain ticket queue, return everything.

          Bouncer.Ticket ticket;
          while ((ticket = bouncerQueue.poll()) != null) {
            ticket.giveBack();
          }

          bouncerQueue = null;
        }

        if (processorYielder != null) {
          processorYielder.close();
          processorYielder = null;
        }
      }
      catch (Throwable e) {
        // No point throwing, since our caller is just a future callback.
        log.noStackTrace().warn(e, "Exception encountered while cleaning up from runAllFully");
      }
      finally {
        // Set finishedFuture after all cleanup is done.
        if (finished.get().isValue()) {
          finishedFuture.set(finished.get().valueOrThrow());
        } else {
          finishedFuture.setException(finished.get().error());
        }
      }
    }
  }
}
