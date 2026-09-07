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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.manager.ProcessorManager;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * Manages execution of {@link FrameProcessor} in an {@link ExecutorService}.
 *
 * If you want single threaded execution, use {@code Execs.singleThreaded()}. It is not a good idea to use this with a
 * same-thread executor like {@code Execs.directExecutor()}, because it will lead to deep call stacks.
 */
public class FrameProcessorExecutor
{
  private static final Logger log = new Logger(FrameProcessorExecutor.class);

  private final ExecutorService exec;

  private final Object lock = new Object();

  // Currently-active cancellationIds.
  @GuardedBy("lock")
  private final Set<String> activeCancellationIds = new HashSet<>();

  // Futures that are active and therefore cancelable.
  // Does not include return futures: those are in cancelableReturnFutures.
  @GuardedBy("lock")
  private final SetMultimap<String, ListenableFuture<?>> cancelableFutures =
      Multimaps.newSetMultimap(new HashMap<>(), Sets::newIdentityHashSet);

  // Return futures that are active and therefore cancelable.
  @GuardedBy("lock")
  private final SetMultimap<String, ListenableFuture<?>> cancelableReturnFutures =
      Multimaps.newSetMultimap(new HashMap<>(), Sets::newIdentityHashSet);

  // Processors that are active and therefore cancelable. They may not currently be running on an actual thread.
  @GuardedBy("lock")
  private final SetMultimap<String, FrameProcessor<?>> cancelableProcessors =
      Multimaps.newSetMultimap(new HashMap<>(), Sets::newIdentityHashSet);

  // Processors that are currently running on a thread.
  // The "lock" is notified when processors are removed from runningProcessors.
  @GuardedBy("lock")
  private final Map<FrameProcessor<?>, Thread> runningProcessors = new IdentityHashMap<>();

  public FrameProcessorExecutor(final ExecutorService exec)
  {
    this.exec = exec;
  }

  /**
   * Runs a processor until it is done, and returns a future that resolves when execution is complete.
   *
   * If "cancellationId" is provided, it must have previously been registered with {@link #registerCancellationId}.
   * Then, it can be used with the {@link #cancel(String)} method to cancel all processors with that
   * same cancellationId.
   */
  public <T> ListenableFuture<T> runFully(final FrameProcessor<T> processor, @Nullable final String cancellationId)
  {
    final List<ReadableFrameChannel> inputChannels = processor.inputChannels();
    final List<WritableFrameChannel> outputChannels = processor.outputChannels();
    final SettableFuture<T> finished = registerCancelableFuture(SettableFuture.create(), true, cancellationId);

    if (finished.isDone()) {
      // Possibly due to starting life out being canceled.
      return finished;
    }

    class ExecutorRunnable implements Runnable
    {
      private final AwaitAnyWidget awaitAnyWidget = new AwaitAnyWidget(inputChannels);

      @Override
      public void run()
      {
        try {
          if (!registerRunningProcessor()) {
            // Processor was canceled. Just exit; cleanup would have been handled elsewhere.
            return;
          }

          // Set nonnull if the processor should run again.
          Runnable nextRun = null;
          Throwable error = null;

          try {
            final List<ListenableFuture<?>> writabilityFutures = gatherWritabilityFutures();
            final List<ListenableFuture<?>> writabilityFuturesToWaitFor =
                writabilityFutures.stream().filter(f -> !f.isDone()).collect(Collectors.toList());

            if (!writabilityFuturesToWaitFor.isEmpty()) {
              logProcessorStatusString(processor, finished.isDone(), null, null, writabilityFutures);
              nextRun = () -> runProcessorAfterFutureResolves(Futures.allAsList(writabilityFuturesToWaitFor), false);
              return;
            }

            final ReturnOrAwait<T> result = runProcessorNow();

            if (result.isReturn()) {
              logProcessorStatusString(processor, finished.isDone(), result, null, null);
              succeed(result.value());
            } else if (result.hasAwaitableFutures()) {
              logProcessorStatusString(processor, finished.isDone(), result, result.awaitableFutures(), null);
              final ListenableFuture<List<Object>> combinedFuture = Futures.allAsList(result.awaitableFutures());
              nextRun = () -> runProcessorAfterFutureResolves(combinedFuture, true);
            } else {
              assert result.hasAwaitableChannels();

              // Don't retain a reference to this set: it may be mutated the next time the processor runs.
              final IntSet await = result.awaitableChannels();

              if (await.isEmpty()) {
                nextRun = () -> exec.execute(ExecutorRunnable.this);
              } else if (result.isAwaitAllChannels() || await.size() == 1) {
                final List<ListenableFuture<?>> readabilityFutures = new ArrayList<>();

                for (final int channelNumber : await) {
                  final ReadableFrameChannel channel = inputChannels.get(channelNumber);
                  if (!channel.isFinished() && !channel.canRead()) {
                    readabilityFutures.add(channel.readabilityFuture());
                  }
                }

                logProcessorStatusString(processor, finished.isDone(), result, readabilityFutures, null);

                if (readabilityFutures.isEmpty()) {
                  nextRun = () -> exec.execute(ExecutorRunnable.this);
                } else {
                  final ListenableFuture<List<Object>> combinedFuture = Futures.allAsList(readabilityFutures);
                  nextRun = () -> runProcessorAfterFutureResolves(combinedFuture, false);
                }
              } else {
                // Await any.
                final ListenableFuture<?> combinedFuture = awaitAnyWidget.awaitAny(await);
                logProcessorStatusString(processor, finished.isDone(), result, List.of(combinedFuture), null);
                nextRun = () -> runProcessorAfterFutureResolves(combinedFuture, false);
              }
            }
          }
          catch (Throwable e) {
            error = e;
          }
          finally {
            final boolean canceled = deregisterRunningProcessor();

            if (canceled) {
              // Processor was canceled while running. Suppress the error and don't schedule the next run;
              // the cancel thread handles cleanup.
            } else if (error != null) {
              fail(error);
            } else if (nextRun != null) {
              nextRun.run();
            }
          }
        }
        catch (Throwable e) {
          fail(e);
        }
      }

      private List<ListenableFuture<?>> gatherWritabilityFutures()
      {
        final List<ListenableFuture<?>> futures = new ArrayList<>();

        for (final WritableFrameChannel channel : outputChannels) {
          futures.add(channel.writabilityFuture());
        }

        return futures;
      }

      /**
       * Executes {@link FrameProcessor#runIncrementally} on the currently-readable inputs.
       * Throws an exception if the processor does.
       */
      private ReturnOrAwait<T> runProcessorNow()
      {
        final IntSet readableInputs = new IntOpenHashSet(inputChannels.size());

        for (int i = 0; i < inputChannels.size(); i++) {
          final ReadableFrameChannel channel = inputChannels.get(i);
          if (channel.isFinished() || channel.canRead()) {
            readableInputs.add(i);
          }
        }

        final String threadName = Thread.currentThread().getName();
        Either<Throwable, ReturnOrAwait<T>> retVal;

        try {
          if (Thread.interrupted()) {
            throw new InterruptedException();
          }

          if (cancellationId != null) {
            // Set the thread name to something involving the cancellationId, to make thread dumps more useful.
            Thread.currentThread().setName(threadName + "-" + cancellationId);
          }

          retVal = Either.value(processor.runIncrementally(readableInputs));
        }
        catch (Throwable e) {
          // Catch InterruptedException too: interrupt was meant for the processor, not us.
          retVal = Either.error(e);
        }
        finally {
          // Restore original thread name.
          Thread.currentThread().setName(threadName);
        }

        return retVal.valueOrThrow();
      }

      /**
       * Schedule this processor to run after the provided future resolves.
       *
       * @param future       the future
       * @param failOnCancel whether the processor should be {@link #fail(Throwable)} if the future is itself canceled.
       *                     This is true for futures provided by {@link ReturnOrAwait#awaitAllFutures(List)},
       *                     because the processor has declared it wants to wait for them; if they are canceled
       *                     the processor must fail. It is false for other futures, which the processor was not
       *                     directly waiting for.
       */
      private <V> void runProcessorAfterFutureResolves(final ListenableFuture<V> future, final boolean failOnCancel)
      {
        final ListenableFuture<V> cancelableFuture = registerCancelableFuture(future, false, cancellationId);

        Futures.addCallback(
            cancelableFuture,
            new FutureCallback<>()
            {
              @Override
              public void onSuccess(final V ignored)
              {
                try {
                  exec.execute(ExecutorRunnable.this);
                }
                catch (Throwable e) {
                  fail(e);
                }
              }

              @Override
              public void onFailure(Throwable t)
              {
                if (failOnCancel || !cancelableFuture.isCancelled()) {
                  fail(t);
                }
              }
            },
            MoreExecutors.directExecutor()
        );
      }

      /**
       * Called when a processor succeeds.
       *
       * Runs the cleanup routine and sets the finished future to a particular value. If cleanup fails, sets the
       * finished future to an error.
       */
      private void succeed(T value)
      {
        try {
          doProcessorCleanup();
        }
        catch (Throwable e) {
          finished.setException(e);
          return;
        }

        finished.set(value);
      }

      /**
       * Called when a processor fails.
       *
       * Cancels output channels, runs the cleanup routine, and sets the finished future to an error.
       */
      private void fail(Throwable e)
      {
        for (final WritableFrameChannel outputChannel : outputChannels) {
          try {
            outputChannel.fail(e);
          }
          catch (Throwable e1) {
            e.addSuppressed(e1);
          }
        }

        try {
          doProcessorCleanup();
        }
        catch (Throwable e1) {
          e.addSuppressed(e1);
        }

        finished.setException(e);
      }

      /**
       * Called when a processor exits via {@link #succeed} or {@link #fail}. Not called when a processor
       * is canceled.
       */
      void doProcessorCleanup() throws IOException
      {
        final boolean doCleanup;

        if (cancellationId != null) {
          synchronized (lock) {
            // Skip cleanup if the processor is no longer in cancelableProcessors. This means one of the "cancel"
            // methods is going to do the cleanup.
            doCleanup = cancelableProcessors.remove(cancellationId, processor);
          }
        } else {
          doCleanup = true;
        }

        if (doCleanup) {
          processor.cleanup();
        }
      }

      /**
       * Registers the current thread as running the processor. Returns false if the processor has been canceled,
       * in which case the caller should not run it.
       */
      private boolean registerRunningProcessor()
      {
        if (cancellationId != null) {
          // After this synchronized block, our thread may be interrupted by cancellations, because "cancel"
          // checks "runningProcessors".
          synchronized (lock) {
            if (cancelableProcessors.containsEntry(cancellationId, processor)) {
              final Thread priorThread = runningProcessors.putIfAbsent(processor, Thread.currentThread());
              if (priorThread != null) {
                throw DruidException.defensive(
                    "Already running processor[%s] on thread[%s], cannot also run on thread[%s]",
                    processor,
                    priorThread.getName(),
                    Thread.currentThread().getName()
                );
              }
            } else {
              // Processor has been canceled. We don't need to handle cleanup, because someone else did it.
              return false;
            }
          }
        }

        return true;
      }

      /**
       * Deregisters the current thread from running the processor, clearing any pending interrupt.
       *
       * @return true if the processor was canceled while it was running
       */
      private boolean deregisterRunningProcessor()
      {
        if (cancellationId != null) {
          // After this synchronized block, our thread will no longer be interrupted by cancellations,
          // because "cancel" checks "runningProcessors".
          synchronized (lock) {
            if (Thread.interrupted()) {
              // ignore: interrupt was meant for the processor, but came after the processor already exited.
            }

            runningProcessors.remove(processor);
            lock.notifyAll();

            return !cancelableProcessors.containsEntry(cancellationId, processor);
          }
        }

        return false;
      }
    }

    final ExecutorRunnable runnable = new ExecutorRunnable();

    finished.addListener(
        () -> {
          // finished will be done here, so this log call gets us the "done=y" log.
          logProcessorStatusString(processor, finished.isDone(), null, null, null);

          // If the future was canceled, and the processor is cancelable, then cancel the processor too.
          if (finished.isCancelled() && cancellationId != null) {
            boolean didRemoveFromCancelableProcessors;

            synchronized (lock) {
              didRemoveFromCancelableProcessors = cancelableProcessors.remove(cancellationId, processor);
            }

            if (didRemoveFromCancelableProcessors) {
              cancel(Collections.singleton(processor));
            }
          }
        },
        Execs.directExecutor()
    );

    logProcessorStatusString(processor, finished.isDone(), null, null, null);
    registerCancelableProcessor(processor, cancellationId);
    exec.execute(runnable);
    return finished;
  }

  /**
   * Runs a sequence of processors and returns a future that resolves when execution is complete. Returns a value
   * accumulated using the provided {@code accumulateFn}.
   *
   * @param processorManager         processors to run
   * @param maxOutstandingProcessors maximum number of processors to run at once
   * @param bouncer                  additional limiter on outstanding processors, beyond maxOutstandingProcessors.
   *                                 Useful when there is some finite resource being shared against multiple different
   *                                 calls to this method.
   * @param cancellationId           optional cancellation id for {@link #runFully}.
   */
  public <T, R> ListenableFuture<R> runAllFully(
      final ProcessorManager<T, ? extends R> processorManager,
      final int maxOutstandingProcessors,
      final Bouncer bouncer,
      @Nullable final String cancellationId
  )
  {
    // Logic resides in a separate class in order to keep this one simpler.
    return new RunAllFullyWidget<T, R>(
        processorManager,
        this,
        maxOutstandingProcessors,
        bouncer,
        cancellationId
    ).run();
  }

  /**
   * Registers a cancellationId, so it can be provided to {@link #runFully} or {@link #runAllFully}. To avoid the
   * set of active cancellationIds growing without bound, callers must also call {@link #cancel(String)} on the
   * same cancellationId when done using it.
   */
  public void registerCancellationId(final String cancellationId)
  {
    synchronized (lock) {
      activeCancellationIds.add(cancellationId);
    }
  }

  /**
   * Deregisters a cancellationId and cancels any currently-running processors associated with that cancellationId.
   * Waits for any canceled processors to exit before returning.
   */
  public void cancel(final String cancellationId)
  {
    Preconditions.checkNotNull(cancellationId, "cancellationId");

    final Set<ListenableFuture<?>> futuresToCancel;
    final Set<FrameProcessor<?>> processorsToCancel;
    final Set<ListenableFuture<?>> returnFuturesToCancel;

    synchronized (lock) {
      activeCancellationIds.remove(cancellationId);
      futuresToCancel = cancelableFutures.removeAll(cancellationId);
      processorsToCancel = cancelableProcessors.removeAll(cancellationId);
      returnFuturesToCancel = cancelableReturnFutures.removeAll(cancellationId);
    }

    // Cancel all associated non-return futures. Do this first, because it prevents new processors from being
    // scheduled by runAllFully.
    for (final ListenableFuture<?> future : futuresToCancel) {
      future.cancel(true);
    }

    // Cancel all processors. Do this before the return futures, because this allows the processors to be canceled
    // all at once, cleanly. Once we start canceling futuresToCancel, we cancel the processor continuation and return
    // futures one-by-one, which if done first would lead to a noisier cancellation.
    cancel(processorsToCancel);

    // Cancel all associated return futures.
    for (final ListenableFuture<?> future : returnFuturesToCancel) {
      future.cancel(true);
    }
  }

  /**
   * Returns an {@link Executor} that executes using the same underlying service, and that is also connected to
   * cancellation through {@link #cancel(String)}.
   *
   * @param cancellationId cancellation ID for the executor
   */
  public Executor asExecutor(@Nullable final String cancellationId)
  {
    return command -> runFully(new RunnableFrameProcessor(command), cancellationId);
  }

  /**
   * Shuts down the underlying executor service immediately.
   */
  public void shutdownNow()
  {
    exec.shutdownNow();
  }

  /**
   * Returns the underlying executor service used by this executor.
   */
  ExecutorService getExecutorService()
  {
    return exec;
  }

  /**
   * Register a future that will be canceled when the provided {@code cancellationId} is canceled.
   *
   * @param future         cancelable future
   * @param isReturn       if this is a return future for a processor; these are canceled last
   * @param cancellationId cancellation id
   */
  <T, FutureType extends ListenableFuture<T>> FutureType registerCancelableFuture(
      final FutureType future,
      final boolean isReturn,
      @Nullable final String cancellationId
  )
  {
    if (cancellationId != null) {
      synchronized (lock) {
        if (!activeCancellationIds.contains(cancellationId)) {
          // Cancel and return immediately.
          future.cancel(true);
          return future;
        }

        final SetMultimap<String, ListenableFuture<?>> map = isReturn ? cancelableReturnFutures : cancelableFutures;
        map.put(cancellationId, future);
        future.addListener(
            () -> {
              synchronized (lock) {
                map.remove(cancellationId, future);
              }
            },
            Execs.directExecutor()
        );
      }
    }

    return future;
  }

  @VisibleForTesting
  int cancelableProcessorCount()
  {
    synchronized (lock) {
      return cancelableProcessors.size();
    }
  }

  /**
   * Cancels a given set of processors. Waits for the processors to exit before returning.
   * Calls {@link FrameProcessor#cleanup()} on all processors.
   *
   * Callers must remove processors from {@link #cancelableProcessors} before calling this method.
   *
   * Logs (but does not throw) exceptions encountered while running {@link FrameProcessor#cleanup()}.
   */
  private void cancel(final Set<FrameProcessor<?>> processorsToCancel)
  {
    boolean interrupted = false;

    synchronized (lock) {
      for (final FrameProcessor<?> processor : processorsToCancel) {
        final Thread processorThread = runningProcessors.get(processor);
        if (processorThread != null) {
          // Interrupt the thread running the processor. Do this under lock, because we want to make sure we don't
          // interrupt the thread shortly *before* or *after* it runs the processor.
          processorThread.interrupt();
        }
      }

      // Wait for all running processors to stop running. Then clean them up outside the critical section.
      while (anyIsRunning(processorsToCancel)) {
        // If lock.wait() is interrupted, remember that but keep waiting. This method needs to proceed onwards
        // even when interrupted, to ensure processor cleanup happens.
        try {
          lock.wait();
        }
        catch (InterruptedException e) {
          interrupted = true;
        }
      }
    }

    // Now processorsToCancel are not running, also won't run again, because the caller removed them
    // from cancelableProcessors. (runProcessorNow checks cancelableProcessors.) Run their cleanup routines
    // outside the critical section.
    for (final FrameProcessor<?> processor : processorsToCancel) {
      // Fail all output channels prior to calling cleanup.
      for (final WritableFrameChannel outputChannel : processor.outputChannels()) {
        try {
          outputChannel.fail(new CancellationException("Canceled"));
        }
        catch (Throwable e) {
          log.debug(e, "Exception encountered while marking output channel failed for processor [%s]", processor);
        }
      }

      try {
        processor.cleanup();
      }
      catch (Throwable e) {
        log.noStackTrace().warn(e, "Exception encountered while canceling processor [%s]", processor);
      }
    }

    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private <T> void registerCancelableProcessor(final FrameProcessor<T> processor, @Nullable final String cancellationId)
  {
    if (cancellationId != null) {
      synchronized (lock) {
        cancelableProcessors.put(cancellationId, processor);
      }
    }
  }

  /**
   * Logs a debug-level status string for the given processor, including input channel readability,
   * output channel writability, and completion state.
   *
   * @param processor the processor
   * @param finished whether the processor's "finished" future has resolved
   * @param returnOrAwait if the processor has just finished {@link FrameProcessor#runIncrementally}, what it returned
   * @param readabilityFutures if the processor has just finished {@link FrameProcessor#runIncrementally}, futures for
   *                           inputs it is waiting for
   * @param writabilityFutures futures for output availability
   */
  private static <T> void logProcessorStatusString(
      final FrameProcessor<T> processor,
      final boolean finished,
      @Nullable final ReturnOrAwait<?> returnOrAwait,
      @Nullable final List<ListenableFuture<?>> readabilityFutures,
      @Nullable final List<ListenableFuture<?>> writabilityFutures
  )
  {
    if (log.isDebugEnabled()) {
      final StringBuilder sb = new StringBuilder()
          .append("Processor [")
          .append(processor)
          .append("]");

      if (returnOrAwait != null) {
        sb.append("; ").append(returnOrAwait);

        if (readabilityFutures != null) {
          // Information about inputs. Note, we can't call methods on the processor's input channels, because
          // they might have been closed.
          sb.append("; in=[");
          for (final ListenableFuture<?> future : readabilityFutures) {
            sb.append(future.isDone() ? 'R' : 'B'); // R = ready; B = blocked.
          }
          sb.append("]");
        }

        if (writabilityFutures != null) {
          // Information about outputs. Note, we can't call methods on the processor's output channels, because
          // they might have been closed.
          sb.append("; out=[");

          for (final ListenableFuture<?> future : writabilityFutures) {
            sb.append(future.isDone() ? 'R' : 'B'); // R = ready; B = blocked.
          }

          sb.append("]");
        }
      }

      sb.append("; done=").append(finished ? "y" : "n");
      log.debug("%s", sb);
    }
  }

  @GuardedBy("lock")
  private boolean anyIsRunning(Set<FrameProcessor<?>> processors)
  {
    for (final FrameProcessor<?> processor : processors) {
      if (runningProcessors.containsKey(processor)) {
        return true;
      }
    }

    return false;
  }
}
