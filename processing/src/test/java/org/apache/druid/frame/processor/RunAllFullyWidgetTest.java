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

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.ReadableNilFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.manager.ProcessorAndCallback;
import org.apache.druid.frame.processor.manager.ProcessorManager;
import org.apache.druid.frame.processor.manager.ProcessorManagers;
import org.apache.druid.frame.processor.test.ChompingFrameProcessor;
import org.apache.druid.frame.processor.test.FailingFrameProcessor;
import org.apache.druid.frame.processor.test.SleepyFrameProcessor;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.TestIndex;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RunWith(Parameterized.class)
public class RunAllFullyWidgetTest extends FrameProcessorExecutorTest.BaseFrameProcessorExecutorTestSuite
{
  private final int bouncerPoolSize;
  private final int maxOutstandingProcessors;
  private final boolean delayed;
  private final AtomicLong closed = new AtomicLong();

  private Bouncer bouncer;

  @GuardedBy("this")
  private int concurrentHighWatermark = 0;

  @GuardedBy("this")
  private int concurrentNow = 0;

  public RunAllFullyWidgetTest(int numThreads, int bouncerPoolSize, int maxOutstandingProcessors, boolean delayed)
  {
    super(numThreads);
    this.bouncerPoolSize = bouncerPoolSize;
    this.maxOutstandingProcessors = maxOutstandingProcessors;
    this.delayed = delayed;
  }

  @Parameterized.Parameters(name =
      "numThreads = {0}, bouncerPoolSize = {1}, maxOutstandingProcessors = {2}, delayed = {3}")
  public static Collection<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();

    for (int numThreads : new int[]{1, 3, 12}) {
      for (int bouncerPoolSize : new int[]{1, 3, 12, Integer.MAX_VALUE}) {
        for (int maxOutstandingProcessors : new int[]{1, 3, 12}) {
          for (boolean delayed : new boolean[]{false, true}) {
            constructors.add(new Object[]{numThreads, bouncerPoolSize, maxOutstandingProcessors, delayed});
          }
        }
      }
    }

    return constructors;
  }

  @Before
  @Override
  public void setUp() throws Exception
  {
    super.setUp();
    bouncer = bouncerPoolSize == Integer.MAX_VALUE ? Bouncer.unlimited() : new Bouncer(bouncerPoolSize);

    synchronized (this) {
      concurrentNow = 0;
      concurrentHighWatermark = 0;
    }
  }

  @After
  @Override
  public void tearDown() throws Exception
  {
    super.tearDown(); // Stops exec, waits for termination

    synchronized (this) {
      Assert.assertEquals(0, concurrentNow);
      MatcherAssert.assertThat(concurrentHighWatermark, Matchers.lessThanOrEqualTo(bouncerPoolSize));
      MatcherAssert.assertThat(concurrentHighWatermark, Matchers.lessThanOrEqualTo(maxOutstandingProcessors));
    }

    Assert.assertEquals("Bouncer current running count", 0, bouncer.getCurrentCount());
    Assert.assertEquals("Bouncer max pool size", bouncerPoolSize, bouncer.getMaxCount());
    Assert.assertEquals("Encountered single close (from ensureClose)", 1, closed.get());
  }

  @Test
  public void test_runAllFully_emptyChannel() throws Exception
  {
    final ListenableFuture<String> future = exec.runAllFully(
        possiblyDelay(ensureClose(ProcessorManagers.none().withAccumulation("xyzzy", (s1, s2) -> s1 + s2))),
        maxOutstandingProcessors,
        bouncer,
        null
    );

    Assert.assertEquals("xyzzy", future.get());
  }

  @Test
  public void test_runAllFully_fiftyThousandProcessors() throws Exception
  {
    final int numProcessors = 100;

    // Doesn't matter what's in this frame.
    final Frame frame =
        Iterables.getOnlyElement(
            FrameSequenceBuilder.fromAdapter(new QueryableIndexStorageAdapter(TestIndex.getMMappedTestIndex()))
                                .frameType(FrameType.ROW_BASED)
                                .frames()
                                .toList()
        );

    final ProcessorManager<Long, Long> processors = ProcessorManagers.of(
        Iterables.transform(
            IntStream.range(0, numProcessors)::iterator,
            i -> {
              final BlockingQueueFrameChannel channel = BlockingQueueFrameChannel.minimal();

              try {
                channel.writable().write(frame);
                channel.writable().close();
              }
              catch (IOException e) {
                throw new RuntimeException(e);
              }

              return new ConcurrencyTrackingFrameProcessor<>(
                  new ChompingFrameProcessor(Collections.singletonList(channel.readable()))
              );
            }
        )
    ).withAccumulation(0L, Long::sum);

    final ListenableFuture<Long> future = exec.runAllFully(
        possiblyDelay(ensureClose(processors)),
        maxOutstandingProcessors,
        bouncer,
        null
    );

    Assert.assertEquals(numProcessors, (long) future.get());
  }

  @Test
  public void test_runAllFully_failing()
  {
    final ListenableFuture<Long> future = exec.runAllFully(
        possiblyDelay(
            ensureClose(
                ProcessorManagers.of(
                    Iterables.transform(
                        IntStream.generate(() -> 0)::iterator /* Infinite stream */,
                        i ->
                            new ConcurrencyTrackingFrameProcessor<>(
                                new FailingFrameProcessor(
                                    ReadableNilFrameChannel.INSTANCE,
                                    BlockingQueueFrameChannel.minimal().writable(),
                                    0
                                )
                            )

                    )
                ).withAccumulation(0L, Long::sum)
            )
        ),
        maxOutstandingProcessors,
        bouncer,
        null
    );

    final ExecutionException e = Assert.assertThrows(ExecutionException.class, future::get);
    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(RuntimeException.class));
    MatcherAssert.assertThat(e.getCause().getCause(), CoreMatchers.instanceOf(RuntimeException.class));
    MatcherAssert.assertThat(
        e.getCause().getCause(),
        ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo("failure!"))
    );
  }

  @Test
  public void test_runAllFully_errorAccumulateFn()
  {
    final ListenableFuture<Long> future = exec.runAllFully(
        possiblyDelay(
            ensureClose(
                ProcessorManagers.of(
                    Iterables.transform(
                        IntStream.range(0, 100)::iterator,
                        i -> new ChompingFrameProcessor(Collections.emptyList())
                    )
                ).withAccumulation(
                    0L,
                    (x, y) -> {
                      throw new ISE("error!");
                    }
                )
            )
        ),
        maxOutstandingProcessors,
        bouncer,
        null
    );

    final ExecutionException e = Assert.assertThrows(ExecutionException.class, future::get);
    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(IllegalStateException.class));
    MatcherAssert.assertThat(e.getCause(), ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo("error!")));
  }

  @Test
  public void test_runAllFully_errorChannelFirstElement()
  {
    final ListenableFuture<Long> future = exec.runAllFully(
        possiblyDelay(
            new ThrowOnNextProcessorManager<>(
                ensureClose(
                    ProcessorManagers.of(
                        Iterables.transform(
                            IntStream.generate(() -> 0)::iterator /* Infinite stream */,
                            i -> new ChompingFrameProcessor(Collections.emptyList())
                        )
                    ).withAccumulation(0L, Long::sum)
                ),
                0
            )
        ),
        maxOutstandingProcessors,
        bouncer,
        null
    );

    final ExecutionException e = Assert.assertThrows(ExecutionException.class, future::get);
    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(IllegalStateException.class));
    MatcherAssert.assertThat(e.getCause(), ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo("error!")));
  }

  @Test
  public void test_runAllFully_errorChannelSecondElement()
  {
    final ListenableFuture<Long> future = exec.runAllFully(
        possiblyDelay(
            new ThrowOnNextProcessorManager<>(
                ensureClose(
                    ProcessorManagers.of(
                        Iterables.transform(
                            IntStream.generate(() -> 0)::iterator /* Infinite stream */,
                            i -> new ChompingFrameProcessor(Collections.emptyList())
                        )
                    ).withAccumulation(0L, Long::sum)
                ),
                1
            )
        ),
        maxOutstandingProcessors,
        bouncer,
        null
    );

    final ExecutionException e = Assert.assertThrows(ExecutionException.class, future::get);
    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(IllegalStateException.class));
    MatcherAssert.assertThat(e.getCause(), ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo("error!")));
  }

  @Test
  public void test_runAllFully_errorChannelHundredthElement()
  {
    final ListenableFuture<Long> future = exec.runAllFully(
        possiblyDelay(
            new ThrowOnNextProcessorManager<>(
                ensureClose(
                    ProcessorManagers.of(
                        Iterables.transform(
                            IntStream.generate(() -> 0)::iterator /* Infinite stream */,
                            i -> new ChompingFrameProcessor(Collections.emptyList())
                        )
                    ).withAccumulation(0L, Long::sum)
                ),
                100
            )
        ),
        maxOutstandingProcessors,
        bouncer,
        null
    );

    final ExecutionException e = Assert.assertThrows(ExecutionException.class, future::get);
    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(IllegalStateException.class));
    MatcherAssert.assertThat(e.getCause(), ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo("error!")));
  }

  @Test
  public void test_runAllFully_errorChannelClose()
  {
    final ListenableFuture<Long> future = exec.runAllFully(
        possiblyDelay(
            new ThrowOnCloseProcessorManager<>(
                ensureClose(
                    ProcessorManagers.of(
                        Iterables.transform(
                            IntStream.range(0, 101)::iterator,
                            i -> new ChompingFrameProcessor(Collections.emptyList())
                        )
                    ).withAccumulation(0L, Long::sum)
                )
            )
        ),
        maxOutstandingProcessors,
        bouncer,
        null
    );

    final ExecutionException e = Assert.assertThrows(ExecutionException.class, future::get);
    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(IllegalStateException.class));
    MatcherAssert.assertThat(e.getCause(), ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo("error!")));
  }

  @Test
  public void test_runAllFully_errorChannelSecondElementAndClose()
  {
    final ListenableFuture<Long> future = exec.runAllFully(
        possiblyDelay(
            new ThrowOnCloseProcessorManager<>(
                new ThrowOnNextProcessorManager<>(
                    ensureClose(
                        ProcessorManagers.of(
                            Iterables.transform(
                                IntStream.range(0, 101)::iterator,
                                i -> new ChompingFrameProcessor(Collections.emptyList())
                            )
                        ).withAccumulation(0L, Long::sum)
                    ),
                    1
                )
            )
        ),
        maxOutstandingProcessors,
        bouncer,
        null
    );

    final ExecutionException e = Assert.assertThrows(ExecutionException.class, future::get);
    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(IllegalStateException.class));
    MatcherAssert.assertThat(e.getCause(), ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo("error!")));
  }

  @Test(timeout = 30_000L)
  @SuppressWarnings("BusyWait")
  public void test_runAllFully_futureCancel() throws InterruptedException
  {
    final int expectedRunningProcessors = Math.min(Math.min(bouncerPoolSize, maxOutstandingProcessors), numThreads);

    final List<SleepyFrameProcessor> processors =
        IntStream.range(0, 10 * expectedRunningProcessors)
                 .mapToObj(i -> new SleepyFrameProcessor())
                 .collect(Collectors.toList());

    final ListenableFuture<Long> future = exec.runAllFully(
        possiblyDelay(
            ensureClose(
                ProcessorManagers.of(Sequences.simple(processors).map(ConcurrencyTrackingFrameProcessor::new))
                                 .withAccumulation(0L, Long::sum)
            )
        ),
        maxOutstandingProcessors,
        bouncer,
        "xyzzy"
    );

    for (int i = 0; i < expectedRunningProcessors; i++) {
      processors.get(i).awaitRun();
    }

    Assert.assertTrue(future.cancel(true));
    Assert.assertTrue(future.isCancelled());

    // We don't have a good way to wait for future cancellation to truly finish. Resort to a waiting-loop.
    while (exec.cancelableProcessorCount() > 0) {
      Thread.sleep(10);
    }

    Assert.assertEquals(0, exec.cancelableProcessorCount());
  }

  /**
   * Wrap in {@link DelayedProcessorManager} if {@link #delayed} is set.
   */
  private <T, R> ProcessorManager<T, R> possiblyDelay(final ProcessorManager<T, R> processorManager)
  {
    return delayed ? new DelayedProcessorManager<>(processorManager) : processorManager;
  }

  /**
   * Ensure that the provided processor manager is closed once, and only once. Must be called once per test case.
   */
  private <T, R> ProcessorManager<T, R> ensureClose(final ProcessorManager<T, R> processorManager)
  {
    return new EnsureCloseProcessorManager<>(processorManager);
  }

  /**
   * FrameProcessor wrapper that updates {@link #concurrentNow}, {@link #concurrentHighWatermark} to enable
   * verification of concurrency controls.
   */
  private class ConcurrencyTrackingFrameProcessor<T> implements FrameProcessor<T>
  {
    // Acquire a pool item to ensure that Bouncer and maxOutstandingProcessors work properly.
    private final AtomicBoolean didRun = new AtomicBoolean(false);
    private final AtomicBoolean didCleanup = new AtomicBoolean(false);
    private final FrameProcessor<T> delegate;

    public ConcurrencyTrackingFrameProcessor(FrameProcessor<T> delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public List<ReadableFrameChannel> inputChannels()
    {
      return delegate.inputChannels();
    }

    @Override
    public List<WritableFrameChannel> outputChannels()
    {
      return delegate.outputChannels();
    }

    @Override
    public ReturnOrAwait<T> runIncrementally(IntSet readableInputs) throws InterruptedException, IOException
    {
      if (didRun.compareAndSet(false, true)) {
        synchronized (RunAllFullyWidgetTest.this) {
          concurrentNow++;

          if (concurrentHighWatermark < concurrentNow) {
            concurrentHighWatermark = concurrentNow;
          }
        }
      }

      return delegate.runIncrementally(readableInputs);
    }

    @Override
    public void cleanup() throws IOException
    {
      try {
        delegate.cleanup();
      }
      finally {
        synchronized (RunAllFullyWidgetTest.this) {
          if (didRun.get()) {
            if (didCleanup.compareAndSet(false, true)) {
              concurrentNow--;
            }
          }
        }
      }
    }
  }

  private class EnsureCloseProcessorManager<T, R> implements ProcessorManager<T, R>
  {
    private final ProcessorManager<T, R> delegate;

    public EnsureCloseProcessorManager(ProcessorManager<T, R> delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public ListenableFuture<Optional<ProcessorAndCallback<T>>> next()
    {
      return delegate.next();
    }

    @Override
    public R result()
    {
      return delegate.result();
    }

    @Override
    public void close()
    {
      closed.getAndIncrement();
      delegate.close();
    }
  }

  /**
   * Processor manager that throws an error on the Nth element.
   */
  private static class ThrowOnNextProcessorManager<T, R> implements ProcessorManager<T, R>
  {
    private final ProcessorManager<T, R> delegate;
    private int i;

    public ThrowOnNextProcessorManager(final ProcessorManager<T, R> delegate, final int i)
    {
      this.delegate = delegate;
      this.i = i;
    }

    @Override
    public ListenableFuture<Optional<ProcessorAndCallback<T>>> next()
    {
      if (i == 0) {
        throw new ISE("error!");
      } else {
        i--;
        return delegate.next();
      }
    }

    @Override
    public R result()
    {
      return delegate.result();
    }

    @Override
    public void close()
    {
      delegate.close();
    }
  }

  /**
   * Processor manager that throws an error on {@link #close()}.
   */
  private static class ThrowOnCloseProcessorManager<T, R> implements ProcessorManager<T, R>
  {
    private final ProcessorManager<T, R> delegate;

    public ThrowOnCloseProcessorManager(ProcessorManager<T, R> delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public ListenableFuture<Optional<ProcessorAndCallback<T>>> next()
    {
      return delegate.next();
    }

    @Override
    public R result()
    {
      return delegate.result();
    }

    @Override
    public void close()
    {
      delegate.close();
      throw new ISE("error!");
    }
  }

  /**
   * Processor manager that effectively delays future resolution by deferring it through {@link #exec}.
   * Especially useful on single-threaded test cases. This helps us ensure that things work when channels don't have
   * processors immediately ready upon a call to {@link FrameProcessorExecutor#runAllFully}.
   */
  private class DelayedProcessorManager<T, R> implements ProcessorManager<T, R>
  {
    private final ProcessorManager<T, R> delegate;

    public DelayedProcessorManager(ProcessorManager<T, R> delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public ListenableFuture<Optional<ProcessorAndCallback<T>>> next()
    {
      final ListenableFuture<Optional<ProcessorAndCallback<T>>> future = delegate.next();
      final SettableFuture<Optional<ProcessorAndCallback<T>>> retVal = SettableFuture.create();

      Futures.addCallback(
          future,
          new FutureCallback<Optional<ProcessorAndCallback<T>>>()
          {
            @Override
            public void onSuccess(Optional<ProcessorAndCallback<T>> result)
            {
              retVal.set(result);
            }

            @Override
            public void onFailure(Throwable t)
            {
              retVal.setException(t);
            }
          },
          exec.getExecutorService()
      );

      return retVal;
    }

    @Override
    public R result()
    {
      return delegate.result();
    }

    @Override
    public void close()
    {
      delegate.close();
    }
  }
}
