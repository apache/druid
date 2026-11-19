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

package org.apache.druid.frame.channel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class ComposingReadableFrameChannelTest
{
  @Test
  public void testFinishedChannelsAreNotReopened()
  {
    final RowsAndColumns rac1 = Mockito.mock(RowsAndColumns.class);
    final RowsAndColumns rac2 = Mockito.mock(RowsAndColumns.class);

    // Each supplier opens a new channel that reads from the beginning, like durable storage channels do.
    final ComposingReadableFrameChannel channel = new ComposingReadableFrameChannel(
        0,
        ImmutableList.of(() -> singleFrameChannel(rac1), () -> singleFrameChannel(rac2)),
        new HashSet<>(ImmutableSet.of(0, 1))
    );

    final List<RowsAndColumns> racsRead = new ArrayList<>();
    while (!channel.isFinished()) {
      if (channel.canRead()) {
        racsRead.add(channel.read());
      }
    }

    Assertions.assertEquals(ImmutableList.of(rac1, rac2), racsRead);
    Assertions.assertTrue(channel.isFinished());
    Assertions.assertFalse(channel.canRead());
    Assertions.assertTrue(channel.readabilityFuture().isDone());
    channel.close();
  }

  private static ReadableFrameChannel singleFrameChannel(final RowsAndColumns rac)
  {
    final BlockingQueueFrameChannel channel = new BlockingQueueFrameChannel(1);
    try {
      channel.writable().write(rac);
      channel.writable().close();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    return channel.readable();
  }
}
