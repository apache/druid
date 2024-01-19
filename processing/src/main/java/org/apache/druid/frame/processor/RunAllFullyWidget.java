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
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.frame.processor.manager.ProcessorAndCallback;
import org.apache.druid.frame.processor.manager.ProcessorManager;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Helper for {@link FrameProcessorExecutor#runAllFully}. See the javadoc for that method for details about the
 * expected behavior of this class.
 *
 * One instance of this class is created for each call to {@link FrameProcessorExecutor#runAllFully}. It gradually
 * executes all processors from the provided sequence using the {@link FrameProcessorExecutor#runFully} method.
 * The {@code bouncer} and {@code maxOutstandingProcessors} parameters are used to control how many processors are
 * executed on the {@link FrameProcessorExecutor} concurrently.
 */
@SuppressWarnings("CheckReturnValue")
public class RunAllFullyWidget<T, ResultType>
{
  private static final Logger log = new Logger(RunAllFullyWidget.class);

  private final ProcessorManager<T, ResultType> processorManager;
  private final FrameProcessorExecutor exec;
  private final int maxOutstandingProcessors;
  private final Bouncer bouncer;
  @Nullable
  private final String cancellationId;

  RunAllFullyWidget(
      ProcessorManager<T, ResultType> processorManager,
      FrameProcessorExecutor exec,
      int maxOutstandingProcessors,
      Bouncer bouncer,
      @Nullable String cancellationId
  )
  {
    this.processorManager = processorManager;
    this.exec = exec;
    this.maxOutstandingProcessors = maxOutstandingProcessors;
    this.bouncer = bouncer;
    this.cancellationId = cancellationId;
  }

  ListenableFuture<ResultType> run()
  {
    final ListenableFuture<Optional<ProcessorAndCallback<T>>> nextProcessorFuture;

    try {
      nextProcessorFuture = processorManager.next();
      if (nextProcessorFuture.isDone() && !nextProcessorFuture.get().isPresent()) {
        // Save some time and return immediately.
        final ResultType retVal = processorManager.result();
        processorManager.close();
        return Futures.immediateFuture(retVal);
      }
    }
    catch (Throwable e) {
      CloseableUtils.closeAndSuppressExceptions(processorManager, e::addSuppressed);
      return Futures.immediateFailedFuture(e);
    }

    // This single RunAllFullyRunnable will be submitted to the executor "maxOutstandingProcessors" times.
    // It runs concurrently with itself.
    final RunAllFullyRunnable runnable = new RunAllFullyRunnable(nextProcessorFuture);

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
    ListenableFuture<Optional<ProcessorAndCallback<T>>> nextProcessorFuture;

    /**
     * Number of processors currently outstanding. Final cleanup is done when there are no more processors in
     * {@link #processorManager}, and when this reaches zero.
     */
    @GuardedBy("runAllFullyLock")
    int outstandingProcessors;

    /**
     * Currently outstanding futures from {@link FrameProcessorExecutor#runFully}. Used for cancellation.
     */
    @GuardedBy("runAllFullyLock")
    Set<ListenableFuture<?>> outstandingFutures = Collections.newSetFromMap(new IdentityHashMap<>());

    /**
     * Tickets from a {@link Bouncer} that are available for use by the next instance of this class to run.
     */
    @Nullable // nulled out by cleanup()
    @GuardedBy("runAllFullyLock")
    Queue<Bouncer.Ticket> bouncerTicketQueue = new ArrayDeque<>();

    /**
     * Whether {@link #cleanup()} has executed.
     */
    @GuardedBy("runAllFullyLock")
    boolean didCleanup;

    private RunAllFullyRunnable(final ListenableFuture<Optional<ProcessorAndCallback<T>>> nextProcessorFuture)
    {
      this.nextProcessorFuture = nextProcessorFuture;
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
      final ProcessorAndCallback<T> nextProcessor;
      Bouncer.Ticket nextTicket = null;

      synchronized (runAllFullyLock) {
        try {
          if (finished.get() != null) {
            cleanupIfNoMoreProcessors();
            return;
          } else {
            if (!nextProcessorFuture.isDone()) {
              final ListenableFuture<Optional<ProcessorAndCallback<T>>> futureRef = nextProcessorFuture;
              // Wait for readability.
              // Note: this code effectively runs *all* outstanding RunAllFullyRunnable when the future resolves, even
              // if only a single processor is available to be run. Still correct, but may be wasteful in situations
              // where a processor manager blocks frequently.
              futureRef.addListener(
                  () -> {
                    if (!futureRef.isCancelled()) {
                      exec.getExecutorService().submit(RunAllFullyRunnable.this);
                    }
                  },
                  Execs.directExecutor()
              );
              return;
            }

            final Optional<ProcessorAndCallback<T>> maybeNextProcessor = nextProcessorFuture.get();

            if (!maybeNextProcessor.isPresent()) {
              // Finished.
              if (outstandingProcessors == 0) {
                finished.compareAndSet(null, Either.value(processorManager.result()));
                cleanupIfNoMoreProcessors();
              }
              return;
            }

            // Next processor is ready to run. Let's do it.
            assert bouncerTicketQueue != null;

            final Bouncer.Ticket ticketFromQueue = bouncerTicketQueue.poll();

            if (ticketFromQueue != null) {
              nextTicket = ticketFromQueue;
            } else {
              final ListenableFuture<Bouncer.Ticket> ticketFuture =
                  exec.registerCancelableFuture(bouncer.ticket(), false, cancellationId);

              if (ticketFuture.isDone() && !ticketFuture.isCancelled()) {
                nextTicket = FutureUtils.getUncheckedImmediately(ticketFuture);
              } else {
                // No ticket available. Run again when there's a ticket.
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
                            bouncerTicketQueue.add(ticket);
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
            nextProcessor = maybeNextProcessor.get();
            nextProcessorFuture = processorManager.next();
            outstandingProcessors++;
          }
        }
        catch (Throwable e) {
          if (nextTicket != null) {
            nextTicket.giveBack();
          }
          finished.compareAndSet(null, Either.error(e));
          cleanupIfNoMoreProcessors();
          return;
        }

        assert nextTicket != null;
        assert nextProcessor != null;

        final ListenableFuture<T> future = exec.runFully(
            FrameProcessors.withBaggage(nextProcessor.processor(), nextTicket::giveBack),
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

                try {
                  synchronized (runAllFullyLock) {
                    outstandingProcessors--;
                    outstandingFutures.remove(future);

                    isDone = outstandingProcessors == 0
                             && nextProcessorFuture.isDone()
                             && !nextProcessorFuture.get().isPresent();

                    nextProcessor.onComplete(result);
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
                  finished.compareAndSet(null, Either.value(processorManager.result()));

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
            },
            MoreExecutors.directExecutor()
        );
      }
    }

    /**
     * Run {@link #cleanup()} if no more processors will be launched.
     */
    @GuardedBy("runAllFullyLock")
    private void cleanupIfNoMoreProcessors()
    {
      if (outstandingProcessors == 0 && finished.get() != null && !didCleanup) {
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

      Throwable caught = null;

      try {
        if (bouncerTicketQueue != null) {
          // Drain ticket queue, return everything.

          Bouncer.Ticket ticket;
          while ((ticket = bouncerTicketQueue.poll()) != null) {
            ticket.giveBack();
          }

          bouncerTicketQueue = null;
        }

        processorManager.close();
      }
      catch (Throwable e) {
        caught = e;
      }
      finally {
        didCleanup = true;

        // Set finishedFuture after all cleanup is done.
        if (finished.get().isValue()) {
          if (caught != null) {
            // Propagate exception caught during cleanup.
            finishedFuture.setException(caught);
          } else {
            finishedFuture.set(finished.get().valueOrThrow());
          }
        } else {
          final Throwable t = finished.get().error();
          if (caught != null) {
            t.addSuppressed(caught);
          }
          finishedFuture.setException(t);
        }
      }
    }
  }
}
