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
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.ReadableFileFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameFileChannel;
import org.apache.druid.frame.file.FrameFile;
import org.apache.druid.frame.file.FrameFileWriter;
import org.apache.druid.frame.processor.test.ChompingFrameProcessor;
import org.apache.druid.frame.processor.test.FailingFrameProcessor;
import org.apache.druid.frame.processor.test.InfiniteFrameProcessor;
import org.apache.druid.frame.processor.test.SleepyFrameProcessor;
import org.apache.druid.frame.processor.test.SuperBlasterFrameProcessor;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.utils.CloseableUtils;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@RunWith(Enclosed.class)
public class FrameProcessorExecutorTest
{
  @RunWith(Parameterized.class)
  public static class SuperBlasterTests extends BaseFrameProcessorExecutorTestSuite
  {
    // Tests in this class use SuperBlasterFrameProcessor, which can exercise various kinds of await styles.
    private final SuperBlasterFrameProcessor.AwaitStyle awaitStyle;

    public SuperBlasterTests(int numThreads, SuperBlasterFrameProcessor.AwaitStyle awaitStyle)
    {
      super(numThreads);
      this.awaitStyle = awaitStyle;
    }

    @Parameterized.Parameters(name = "numThreads = {0}, awaitStyle = {1}")
    public static Collection<Object[]> constructorFeeder()
    {
      final List<Object[]> constructors = new ArrayList<>();

      for (int numThreads : new int[]{1, 3, 12}) {
        for (SuperBlasterFrameProcessor.AwaitStyle awaitStyle : SuperBlasterFrameProcessor.AwaitStyle.values()) {
          constructors.add(new Object[]{numThreads, awaitStyle});
        }
      }

      return constructors;
    }

    @Test
    public void test_runFully() throws Exception
    {
      // 3 input files blasted to 2 outputs (2 copies of the data), then muxed to one file.

      final IncrementalIndexStorageAdapter adapter =
          new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());
      final List<File> inFiles = writeToNFiles(adapter, 3);
      final File outFile = temporaryFolder.newFile();

      final BlockingQueueFrameChannel memoryChannel1 = BlockingQueueFrameChannel.minimal();
      final BlockingQueueFrameChannel memoryChannel2 = BlockingQueueFrameChannel.minimal();

      final SuperBlasterFrameProcessor blaster = new SuperBlasterFrameProcessor(
          inFiles.stream().map(FrameProcessorExecutorTest::openFileChannel).collect(Collectors.toList()),
          ImmutableList.of(memoryChannel1.writable(), memoryChannel2.writable()),
          awaitStyle
      );

      final FrameChannelMuxer muxer = new FrameChannelMuxer(
          ImmutableList.of(memoryChannel1.readable(), memoryChannel2.readable()),
          new WritableFrameFileChannel(
              FrameFileWriter.open(
                  Channels.newChannel(Files.newOutputStream(outFile.toPath())),
                  null
              )
          )
      );

      final ListenableFuture<Long> blasterFuture = exec.runFully(blaster, null);
      final ListenableFuture<Long> muxerFuture = exec.runFully(muxer, null);

      Assert.assertEquals(adapter.getNumRows(), (long) blasterFuture.get());
      Assert.assertEquals(adapter.getNumRows() * 2, (long) muxerFuture.get());

      Assert.assertEquals(
          adapter.getNumRows() * 2,
          FrameTestUtil.readRowsFromFrameChannel(
              new ReadableFileFrameChannel(FrameFile.open(outFile)),
              FrameReader.create(adapter.getRowSignature())
          ).toList().size()
      );
    }
  }

  @RunWith(Parameterized.class)
  public static class MiscTests extends BaseFrameProcessorExecutorTestSuite
  {
    public MiscTests(int numThreads)
    {
      super(numThreads);
    }

    @Parameterized.Parameters(name = "numThreads = {0}")
    public static Collection<Object[]> constructorFeeder()
    {
      final List<Object[]> constructors = new ArrayList<>();

      for (int numThreads : new int[]{1, 3, 12}) {
        constructors.add(new Object[]{numThreads});
      }

      return constructors;
    }

    @Test
    public void test_runFully_errors() throws Exception
    {
      final IncrementalIndexStorageAdapter adapter =
          new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());
      final File inFile = Iterables.getOnlyElement(writeToNFiles(adapter, 1));
      final ReadableFrameChannel inChannel = openFileChannel(inFile);
      final BlockingQueueFrameChannel outChannel = BlockingQueueFrameChannel.minimal();

      final FailingFrameProcessor failer = new FailingFrameProcessor(inChannel, outChannel.writable(), 0);
      final ListenableFuture<Long> failerFuture = exec.runFully(failer, null);

      final ExecutionException e = Assert.assertThrows(
          ExecutionException.class,
          failerFuture::get
      );

      MatcherAssert.assertThat(
          e.getCause().getCause(),
          ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("failure!"))
      );

      final ReadableFrameChannel outReadableChannel = outChannel.readable();
      Assert.assertTrue(outReadableChannel.canRead());

      final RuntimeException readException = Assert.assertThrows(
          RuntimeException.class,
          outReadableChannel::read
      );

      MatcherAssert.assertThat(
          readException.getCause(),
          ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("failure!"))
      );

      Assert.assertTrue(outReadableChannel.isFinished()); // Finished now that we read the error
    }

    @Test
    public void test_registerCancelableFuture() throws InterruptedException
    {
      final SettableFuture<Object> future = SettableFuture.create();
      final String cancellationId = "xyzzy";

      Assert.assertSame(future, exec.registerCancelableFuture(future, false, cancellationId));
      exec.cancel(cancellationId);

      // Don't wait for the future to resolve, because exec.cancel should have done that.
      // If we see an unresolved future here, it's a bug in exec.cancel.
      Assert.assertTrue(future.isDone());
      Assert.assertTrue(future.isCancelled());
    }

    @Test
    public void test_cancel_sleepy() throws Exception
    {
      final SleepyFrameProcessor processor = new SleepyFrameProcessor();
      final String cancellationId = "xyzzy";
      final ListenableFuture<Long> future = exec.runFully(processor, cancellationId);

      processor.awaitRun();
      exec.cancel(cancellationId);

      // Don't wait for the future to resolve, because exec.cancel should have done that.
      // If we see an unresolved future here, it's a bug in exec.cancel.
      Assert.assertTrue(future.isDone());
      Assert.assertTrue(future.isCancelled());
      Assert.assertTrue(processor.didGetInterrupt());
      Assert.assertTrue(processor.didCleanup());
    }

    @Test(timeout = 30_000L)
    public void test_futureCancel_sleepy() throws Exception
    {
      final SleepyFrameProcessor processor = new SleepyFrameProcessor();
      final String cancellationId = "xyzzy";
      final ListenableFuture<Long> future = exec.runFully(processor, cancellationId);

      processor.awaitRun();
      Assert.assertTrue(future.cancel(true));
      Assert.assertTrue(future.isDone());
      Assert.assertTrue(future.isCancelled());

      processor.awaitCleanup(); // If this times out, it's a bug that means cancellation didn't happen as expected.
      Assert.assertTrue(processor.didGetInterrupt());
      Assert.assertTrue(processor.didCleanup());
    }

    @Test
    public void test_cancel_concurrency() throws Exception
    {
      final int numSystems = 1000;
      final int numGeneratorsPerSystem = 8;

      // Doesn't matter what's in this frame.
      final Frame frame =
          Iterables.getOnlyElement(
              FrameSequenceBuilder.fromAdapter(new QueryableIndexStorageAdapter(TestIndex.getMMappedTestIndex()))
                                  .frameType(FrameType.ROW_BASED)
                                  .frames()
                                  .toList()
          );

      // Set up many generators that all feed into a chomper.
      final Set<String> systemIds = new HashSet<>();
      final Map<String, List<InfiniteFrameProcessor>> systemGeneratorsMap = new HashMap<>();
      final Map<String, ChompingFrameProcessor> systemChomperMap = new HashMap<>();
      final Map<FrameProcessor<?>, ListenableFuture<Long>> processorFutureMap = new IdentityHashMap<>();
      final Map<String, Boolean> systemCleanStopMap = new HashMap<>();

      // Cancel half of the systems, stop the other half cleanly.
      boolean doCleanStop = false;

      for (int systemNumber = 0; systemNumber < numSystems; systemNumber++) {
        final String systemId = UUID.randomUUID().toString();

        final List<InfiniteFrameProcessor> generators = new ArrayList<>(numGeneratorsPerSystem);
        final List<ReadableFrameChannel> channels = new ArrayList<>(numGeneratorsPerSystem);

        for (int i = 0; i < numGeneratorsPerSystem; i++) {
          final BlockingQueueFrameChannel channel = BlockingQueueFrameChannel.minimal();
          generators.add(new InfiniteFrameProcessor(frame, channel.writable()));
          channels.add(channel.readable());
        }

        final ChompingFrameProcessor chomper = new ChompingFrameProcessor(channels);

        systemIds.add(systemId);
        systemGeneratorsMap.put(systemId, generators);
        systemChomperMap.put(systemId, chomper);
        systemCleanStopMap.put(systemId, doCleanStop);
        doCleanStop = !doCleanStop;
      }

      // Start up all systems at once.
      for (final String systemId : systemGeneratorsMap.keySet()) {
        for (InfiniteFrameProcessor generator : systemGeneratorsMap.get(systemId)) {
          processorFutureMap.put(generator, exec.runFully(generator, systemId));
        }

        final ChompingFrameProcessor chomper = systemChomperMap.get(systemId);
        processorFutureMap.put(chomper, exec.runFully(chomper, systemId));
      }

      // Stop the systems (either cleanly or through cancellation).
      for (Map.Entry<String, ChompingFrameProcessor> entry : systemChomperMap.entrySet()) {
        final String systemId = entry.getKey();

        // Read at least one frame in the chomper before stopping the system.
        entry.getValue().awaitRead();

        if (systemCleanStopMap.get(systemId)) {
          systemGeneratorsMap.get(systemId).forEach(InfiniteFrameProcessor::stop);
        } else {
          exec.cancel(systemId);
        }
      }

      // Verify results of each system.
      for (final String systemId : systemIds) {
        final boolean cleanStop = systemCleanStopMap.get(systemId);
        final List<InfiniteFrameProcessor> generators = systemGeneratorsMap.get(systemId);
        final ChompingFrameProcessor chomper = systemChomperMap.get(systemId);

        if (cleanStop) {
          // Check for clean exit.
          long systemFrameCount = 0;

          // Verify numFrames on each generator.
          for (final InfiniteFrameProcessor generator : generators) {
            final Long retVal = processorFutureMap.get(generator).get();
            Assert.assertNotNull(retVal);
            Assert.assertEquals(generator.getNumFrames(), (long) retVal);
            systemFrameCount += retVal;
          }

          // Verify return value of the chomper.
          final Long retVal = processorFutureMap.get(chomper).get();
          Assert.assertNotNull(retVal);
          Assert.assertEquals(systemFrameCount, (long) retVal);
        } else {
          // Check for cancellation.
          final List<FrameProcessor<?>> allProcessors =
              ImmutableList.copyOf(Iterables.concat(generators, Collections.singleton(chomper)));

          for (FrameProcessor<?> processor : allProcessors) {
            final ListenableFuture<Long> future = processorFutureMap.get(processor);

            // Don't wait for the future to resolve, because exec.cancel should have done that.
            // If we see an unresolved future here, it's a bug in exec.cancel.
            Assert.assertTrue(future.isDone());
            Assert.assertTrue(future.isCancelled());

            final Exception e = Assert.assertThrows(Exception.class, future::get);
            MatcherAssert.assertThat(e, CoreMatchers.instanceOf(CancellationException.class));
          }
        }

        // In both cases, check for cleanup.
        for (final InfiniteFrameProcessor generator : generators) {
          Assert.assertTrue(generator.didCleanup());
        }

        Assert.assertTrue(chomper.didCleanup());
      }
    }

    @Test
    public void test_cancel_nonexistentCancellationId() throws InterruptedException
    {
      // Just making sure no error is thrown when we refer to a nonexistent cancellationId.
      exec.cancel("nonexistent");
    }
  }

  public abstract static class BaseFrameProcessorExecutorTestSuite extends InitializedNullHandlingTest
  {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();
    public final int numThreads;

    FrameProcessorExecutor exec;

    public BaseFrameProcessorExecutorTestSuite(int numThreads)
    {
      this.numThreads = numThreads;
    }

    @Before
    public void setUp() throws Exception
    {
      exec = new FrameProcessorExecutor(
          MoreExecutors.listeningDecorator(
              Execs.multiThreaded(
                  numThreads,
                  StringUtils.encodeForFormat(getClass().getName()) + "-%s"
              )
          )
      );
    }

    @After
    public void tearDown() throws Exception
    {
      exec.getExecutorService().shutdownNow();
      if (!exec.getExecutorService().awaitTermination(1, TimeUnit.MINUTES)) {
        throw new ISE("Executor service did not terminate within 1 minute");
      }
    }

    List<File> writeToNFiles(final StorageAdapter adapter, final int numFiles) throws IOException
    {
      final List<File> files = new ArrayList<>();
      final List<FrameFileWriter> writers = new ArrayList<>();

      try {
        // Set up input files.
        for (int i = 0; i < numFiles; i++) {
          files.add(temporaryFolder.newFile());
          writers.add(
              FrameFileWriter.open(
                  Channels.newChannel(Files.newOutputStream(files.get(i).toPath())),
                  null
              )
          );
        }

        // Write to input files.
        final Consumer<Frame> writer = new Consumer<Frame>()
        {
          private int j = 0;

          @Override
          public void accept(Frame frame)
          {
            try {
              writers.get(j % writers.size()).writeFrame(frame, FrameFileWriter.NO_PARTITION);
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }

            j++;
          }
        };

        FrameSequenceBuilder
            .fromAdapter(adapter)
            .frameType(FrameType.ROW_BASED)
            .allocator(ArenaMemoryAllocator.createOnHeap(1_000_000))
            .maxRowsPerFrame(3)
            .frames()
            .forEach(writer);
      }
      finally {
        CloseableUtils.closeAll(writers);
      }

      return files;
    }
  }

  private static ReadableFrameChannel openFileChannel(final File file)
  {
    try {
      return new ReadableFileFrameChannel(FrameFile.open(file));
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
