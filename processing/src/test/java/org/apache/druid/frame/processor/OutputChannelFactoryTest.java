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
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.channel.FrameWithPartition;
import org.apache.druid.frame.channel.PartitionedReadableFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndexCursorFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public abstract class OutputChannelFactoryTest extends InitializedNullHandlingTest
{
  private final OutputChannelFactory outputChannelFactory;
  private final long frameSize;
  protected final CursorFactory sourceCursorFactory;
  protected final Frame frame;

  public OutputChannelFactoryTest(OutputChannelFactory outputChannelFactory, long frameSize)
  {
    this.outputChannelFactory = outputChannelFactory;
    this.frameSize = frameSize;
    this.sourceCursorFactory = new IncrementalIndexCursorFactory(TestIndex.getIncrementalTestIndex());
    this.frame = Iterables.getOnlyElement(FrameSequenceBuilder.fromCursorFactory(sourceCursorFactory)
                                                               .frameType(FrameType.COLUMNAR)
                                                               .frames()
                                                               .toList());
  }

  @Test
  public void test_openChannel() throws IOException, ExecutionException, InterruptedException
  {
    OutputChannel channel = outputChannelFactory.openChannel(1);

    Assert.assertEquals(1, channel.getPartitionNumber());

    // write data to the channel
    WritableFrameChannel writableFrameChannel = channel.getWritableChannel();
    writableFrameChannel.writabilityFuture().get();
    writableFrameChannel.write(new FrameWithPartition(frame, 1));
    writableFrameChannel.close();

    // read back data from the channel
    verifySingleFrameReadableChannel(
        channel.getReadableChannel(),
        sourceCursorFactory
    );
    Assert.assertEquals(frameSize, channel.getFrameMemoryAllocator().capacity());
  }

  @Test
  public void test_openPartitionedChannel() throws IOException, ExecutionException, InterruptedException
  {
    PartitionedOutputChannel channel = outputChannelFactory.openPartitionedChannel("test", true);
    int[] partitions = new int[]{1, 2};

    // write data to the channel
    WritableFrameChannel writableFrameChannel = channel.getWritableChannel();
    writableFrameChannel.writabilityFuture().get();
    for (int partition : partitions) {
      writableFrameChannel.write(new FrameWithPartition(frame, partition));
    }
    writableFrameChannel.close();

    // read back data from the channel
    Supplier<PartitionedReadableFrameChannel> partitionedReadableFrameChannelSupplier = channel.getReadableChannelSupplier();
    for (int partition : partitions) {
      verifySingleFrameReadableChannel(
          partitionedReadableFrameChannelSupplier.get().getReadableFrameChannel(partition),
          sourceCursorFactory
      );
      Assert.assertEquals(frameSize, channel.getFrameMemoryAllocator().capacity());
    }
  }

  protected void verifySingleFrameReadableChannel(
      ReadableFrameChannel readableFrameChannel,
      CursorFactory cursorFactory
  ) throws ExecutionException, InterruptedException
  {
    readableFrameChannel.readabilityFuture().get();
    // TODO : this is bad. but it is because the input stream channel doesn't honor the contract of readibility future
    // either add timeout to the loop or fix input stream channel
    while (true) {
      if (readableFrameChannel.canRead()) {
        break;
      }
    }
    Frame readbackFrame = readableFrameChannel.read();
    readableFrameChannel.readabilityFuture().get();
    Assert.assertFalse(readableFrameChannel.canRead());
    Assert.assertTrue(readableFrameChannel.isFinished());
    readableFrameChannel.close();

    CursorFactory frameCursorFactory = FrameReader.create(cursorFactory.getRowSignature()).makeCursorFactory(readbackFrame);
    // build list of rows from written and read data to verify
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(CursorBuildSpec.FULL_SCAN);
         final CursorHolder frameHolder = frameCursorFactory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)
    ) {
      List<List<Object>> writtenData = FrameTestUtil.readRowsFromCursor(
          cursorHolder.asCursor(),
          cursorFactory.getRowSignature()
      ).toList();
      List<List<Object>> readData = FrameTestUtil.readRowsFromCursor(
          frameHolder.asCursor(),
          frameCursorFactory.getRowSignature()
      ).toList();

      Assert.assertEquals(
          "Read rows count is different from written rows count",
          writtenData.size(),
          readData.size()
      );
      Assert.assertEquals("Read data is different from written data", writtenData, readData);
    }
  }

  @Test
  public void test_openNilChannel()
  {
    final OutputChannel channel = outputChannelFactory.openNilChannel(1);

    Assert.assertEquals(1, channel.getPartitionNumber());
    Assert.assertTrue(channel.getReadableChannel().isFinished());
    Assert.assertThrows(IllegalStateException.class, channel::getWritableChannel);
  }
}
