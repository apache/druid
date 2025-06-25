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
import org.apache.druid.frame.channel.ByteTracker;
import org.apache.druid.frame.channel.FrameWithPartition;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class ComposingOutputChannelFactoryTest extends OutputChannelFactoryTest
{
  @ClassRule
  public static TemporaryFolder folder = new TemporaryFolder();

  public ComposingOutputChannelFactoryTest() throws IOException
  {
    super(
        new ComposingOutputChannelFactory(
            ImmutableList.of(
                // TODO : currently hardcoded 256k since it allows one frame to be written to each factory
                // nicer to do that automatically
                new FileOutputChannelFactory(folder.newFolder(), 100, new ByteTracker(256_000)),
                new FileOutputChannelFactory(folder.newFolder(), 100, new ByteTracker(256_000))
            ),
            100
        ),
        100
    );
  }

  @Test
  public void test_openChannel2() throws IOException, ExecutionException, InterruptedException
  {
    ComposingOutputChannelFactory outputChannelFactory = new ComposingOutputChannelFactory(
        ImmutableList.of(
            new FileOutputChannelFactory(folder.newFolder(), 100, new ByteTracker(1)),
            new ThrowingOutputChannelFactory() // adding this to check if it gets called
        ),
        100
    );
    OutputChannel channel = outputChannelFactory.openChannel(1);

    Assert.assertEquals(1, channel.getPartitionNumber());
    WritableFrameChannel writableFrameChannel = channel.getWritableChannel();
    writableFrameChannel.writabilityFuture().get();
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> writableFrameChannel.write(new FrameWithPartition(frame, 1))
    );
  }

  @Test
  public void test_openChannel3() throws IOException, ExecutionException, InterruptedException
  {
    // this test checks that the throwing output channels are never opened since 1MB limit on file output channel
    // can handle the test data frames
    ComposingOutputChannelFactory outputChannelFactory = new ComposingOutputChannelFactory(
        ImmutableList.of(
            new FileOutputChannelFactory(folder.newFolder(), 100, new ByteTracker(1_000_000)),
            new ThrowingOutputChannelFactory()
        ),
        100
    );
    OutputChannel channel = outputChannelFactory.openChannel(1);

    Assert.assertEquals(1, channel.getPartitionNumber());
    WritableFrameChannel writableFrameChannel = channel.getWritableChannel();
    writableFrameChannel.writabilityFuture().get();
    writableFrameChannel.write(new FrameWithPartition(frame, 1));
    writableFrameChannel.close();

    verifySingleFrameReadableChannel(channel.getReadableChannel(), sourceCursorFactory);
  }


  private static class ThrowingOutputChannelFactory implements OutputChannelFactory
  {

    @Override
    public OutputChannel openChannel(int partitionNumber)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public PartitionedOutputChannel openPartitionedChannel(String name, boolean deleteAfterRead)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public OutputChannel openNilChannel(int partitionNumber)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBuffered()
    {
      throw new UnsupportedOperationException();
    }
  }
}
