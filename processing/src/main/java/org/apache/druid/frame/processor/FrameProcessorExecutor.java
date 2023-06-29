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
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
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

  private final ListeningExecutorService exec;

  private final Object lock = new Object();

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

  public FrameProcessorExecutor(final ListeningExecutorService exec)
  {
    this.exec = exec;
  }

  /**
   * Returns the underlying executor service used by this executor.
   */
  public ListeningExecutorService getExecutorService()
  {
    return exec;
  }

  /**
   * Runs a processor until it is done, and returns a future that resolves when execution is complete.
   *
   * If "cancellationId" is provided, it can be used with the {@link #cancel(String)} method to cancel all processors
   * currently running with the same cancellationId.
   */
  public <T> ListenableFuture<T> runFully(final FrameProcessor<T> processor, @Nullable final String cancellationId)
  {
    final List<ReadableFrameChannel> inputChannels = processor.inputChannels();
    final List<WritableFrameChannel> outputChannels = processor.outputChannels();
    final SettableFuture<T> finished = registerCancelableFuture(SettableFuture.create(), true, cancellationId);

    class ExecutorRunnable implements Runnable
    {
      private final AwaitAnyWidget awaitAnyWidget = new AwaitAnyWidget(inputChannels);

      @Override
      public void run()
      {
        try {
          final List<ListenableFuture<?>> allWritabilityFutures = gatherWritabilityFutures();
          final List<ListenableFuture<?>> writabilityFuturesToWaitFor =
              allWritabilityFutures.stream().filter(f -> !f.isDone()).collect(Collectors.toList());

          logProcessorStatusString(processor, finished, allWritabilityFutures);

          if (!writabilityFuturesToWaitFor.isEmpty()) {
            runProcessorAfterFutureResolves(Futures.allAsList(writabilityFuturesToWaitFor));
            return;
          }

          final Optional<ReturnOrAwait<T>> maybeResult = runProcessorNow();

          if (!maybeResult.isPresent()) {
            // Processor exited abnormally. Just exit; cleanup would have been handled elsewhere.
            return;
          }

          final ReturnOrAwait<T> result = maybeResult.get();
          logProcessorStatusString(processor, finished, null);

          if (result.isReturn()) {
            succeed(result.value());
          } else {
            // Don't retain a reference to this set: it may be mutated the next time the processor runs.
            final IntSet await = result.awaitSet();

            if (await.isEmpty()) {
              exec.submit(ExecutorRunnable.this);
            } else if (result.isAwaitAll() || await.size() == 1) {
              final List<ListenableFuture<?>> readabilityFutures = new ArrayList<>();

              for (final int channelNumber : await) {
                final ReadableFrameChannel channel = inputChannels.get(channelNumber);
                if (!channel.isFinished() && !channel.canRead()) {
                  readabilityFutures.add(channel.readabilityFuture());
                }
              }

              if (readabilityFutures.isEmpty()) {
                exec.submit(ExecutorRunnable.this);
              } else {
                runProcessorAfterFutureResolves(Futures.allAsList(readabilityFutures));
              }
            } else {
              // Await any.
              runProcessorAfterFutureResolves(awaitAnyWidget.awaitAny(await));
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
       * Executes {@link FrameProcessor#runIncrementally} on the currently-readable inputs, while respecting
       * cancellation. Returns an empty Optional if the processor exited abnormally (canceled or failed). Returns a
       * present Optional if the processor ran successfully. Throws an exception if the processor does.
       */
      private Optional<ReturnOrAwait<T>> runProcessorNow()
      {
        final IntSet readableInputs = new IntOpenHashSet(inputChannels.size());

        for (int i = 0; i < inputChannels.size(); i++) {
          final ReadableFrameChannel channel = inputChannels.get(i);
          if (channel.isFinished() || channel.canRead()) {
            readableInputs.add(i);
          }
        }

        if (cancellationId != null) {
          // After this synchronized block, our thread may be interrupted by cancellations, because "cancel"
          // checks "runningProcessors".
          synchronized (lock) {
            if (cancelableProcessors.containsEntry(cancellationId, processor)) {
              runningProcessors.put(processor, Thread.currentThread());
            } else {
              // Processor has been canceled. We don't need to handle cleanup, because someone else did it.
              return Optional.empty();
            }
          }
        }

        boolean canceled = false;
        Either<Throwable, ReturnOrAwait<T>> retVal;

        try {
          if (Thread.interrupted()) {
            throw new InterruptedException();
          }

          retVal = Either.value(processor.runIncrementally(readableInputs));
        }
        catch (Throwable e) {
          // Catch InterruptedException too: interrupt was meant for the processor, not us.
          retVal = Either.error(e);
        }
        finally {
          if (cancellationId != null) {
            // After this synchronized block, our thread will no longer be interrupted by cancellations,
            // because "cancel" checks "runningProcessors".
            synchronized (lock) {
              if (Thread.interrupted()) {
                // ignore: interrupt was meant for the processor, but came after the processor already exited.
              }

              runningProcessors.remove(processor);
              lock.notifyAll();

              if (!cancelableProcessors.containsEntry(cancellationId, processor)) {
                // Processor has been canceled by one of the "cancel" methods. They will handle cleanup.
                canceled = true;
              }
            }
          }
        }

        if (canceled) {
          return Optional.empty();
        } else {
          return Optional.of(retVal.valueOrThrow());
        }
      }

      private <V> void runProcessorAfterFutureResolves(final ListenableFuture<V> future)
      {
        final ListenableFuture<V> cancelableFuture = registerCancelableFuture(future, false, cancellationId);

        Futures.addCallback(
            cancelableFuture,
            new FutureCallback<V>()
            {
              @Override
              public void onSuccess(final V ignored)
              {
                try {
                  exec.submit(ExecutorRunnable.this);
                }
                catch (Throwable e) {
                  fail(e);
                }
              }

              @Override
              public void onFailure(Throwable t)
              {
                // Ignore cancellation.
                if (!cancelableFuture.isCancelled()) {
                  fail(t);
                }
              }
            }
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
    }

    final ExecutorRunnable runnable = new ExecutorRunnable();

    finished.addListener(
        () -> {
          logProcessorStatusString(processor, finished, null);

          // If the future was canceled, and the processor is cancelable, then cancel the processor too.
          if (finished.isCancelled() && cancellationId != null) {
            boolean didRemoveFromCancelableProcessors;

            synchronized (lock) {
              didRemoveFromCancelableProcessors = cancelableProcessors.remove(cancellationId, processor);
            }

            if (didRemoveFromCancelableProcessors) {
              try {
                cancel(Collections.singleton(processor));
              }
              catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }
          }
        },
        Execs.directExecutor()
    );

    logProcessorStatusString(processor, finished, null);
    registerCancelableProcessor(processor, cancellationId);
    exec.submit(runnable);
    return finished;
  }

  /**
   * Runs a sequence of processors and returns a future that resolves when execution is complete. Returns a value
   * accumulated using the provided {@code accumulateFn}.
   *
   * @param processors               sequence of processors to run
   * @param initialResult            initial value for result accumulation. If there are no processors at all, this
   *                                 is returned.
   * @param accumulateFn             result accumulator. Applied in a critical section, so it should be something
   *                                 that executes quickly. It gets {@code initialResult} for the initial accumulation.
   * @param maxOutstandingProcessors maximum number of processors to run at once
   * @param bouncer                  additional limiter on outstanding processors, beyond maxOutstandingProcessors.
   *                                 Useful when there is some finite resource being shared against multiple different
   *                                 calls to this method.
   * @param cancellationId           optional cancellation id for {@link #runFully}.
   */
  public <T, ResultType> ListenableFuture<ResultType> runAllFully(
      final Sequence<? extends FrameProcessor<T>> processors,
      final ResultType initialResult,
      final BiFunction<ResultType, T, ResultType> accumulateFn,
      final int maxOutstandingProcessors,
      final Bouncer bouncer,
      @Nullable final String cancellationId
  )
  {
    // Logic resides in a separate class in order to keep this one simpler.
    return new RunAllFullyWidget<>(
        processors,
        this,
        initialResult,
        accumulateFn,
        maxOutstandingProcessors,
        bouncer,
        cancellationId
    ).run();
  }

  /**
   * Cancels all processors associated with a given cancellationId. Waits for the processors to exit before
   * returning.
   */
  public void cancel(final String cancellationId) throws InterruptedException
  {
    Preconditions.checkNotNull(cancellationId, "cancellationId");

    final Set<ListenableFuture<?>> futuresToCancel;
    final Set<FrameProcessor<?>> processorsToCancel;
    final Set<ListenableFuture<?>> returnFuturesToCancel;

    synchronized (lock) {
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
      throws InterruptedException
  {
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
      while (processorsToCancel.stream().anyMatch(runningProcessors::containsKey)) {
        lock.wait();
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
  }

  private <T> void registerCancelableProcessor(final FrameProcessor<T> processor, @Nullable final String cancellationId)
  {
    if (cancellationId != null) {
      synchronized (lock) {
        cancelableProcessors.put(cancellationId, processor);
      }
    }
  }

  private static <T> void logProcessorStatusString(
      final FrameProcessor<T> processor,
      final ListenableFuture<?> finishedFuture,
      @Nullable final List<ListenableFuture<?>> writabilityFutures
  )
  {
    if (log.isDebugEnabled()) {
      final StringBuilder sb = new StringBuilder()
          .append("Processor [")
          .append(processor)
          .append("]; in=[");

      for (ReadableFrameChannel channel : processor.inputChannels()) {
        if (channel.canRead()) {
          sb.append("R"); // R for readable
        } else if (channel.isFinished()) {
          sb.append("D"); // D for done
        } else {
          sb.append("~"); // ~ for waiting
        }
      }

      sb.append("]");

      if (writabilityFutures != null) {
        sb.append("; out=[");

        for (final ListenableFuture<?> future : writabilityFutures) {
          if (future.isDone()) {
            sb.append("W"); // W for writable
          } else {
            sb.append("~"); // ~ for waiting
          }
        }

        sb.append("]");
      }

      sb.append("; cancel=").append(finishedFuture.isCancelled() ? "y" : "n");
      sb.append("; done=").append(finishedFuture.isDone() ? "y" : "n");

      log.debug(StringUtils.encodeForFormat(sb.toString()));
    }
  }
}
