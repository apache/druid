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

import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ReadableConcatFrameChannelTest extends InitializedNullHandlingTest
{
  @Test
  public void testChannel() throws Exception
  {
    final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(TestIndex.getMMappedTestIndex());
    final List<Frame> frames =
        FrameSequenceBuilder.fromAdapter(adapter)
                            .frameType(FrameType.ROW_BASED)
                            .maxRowsPerFrame(11)
                            .frames()
                            .toList();

    final List<ReadableFrameChannel> channels = new ArrayList<>();
    for (final Frame frame : frames) {
      final BlockingQueueFrameChannel channel = BlockingQueueFrameChannel.minimal();
      channel.writable().write(frame);
      channel.writable().close();
      channels.add(channel.readable());

      // Sprinkle in some empty channels too, to make sure they work.
      channels.add(ReadableNilFrameChannel.INSTANCE);
    }

    final ReadableConcatFrameChannel concatChannel = ReadableConcatFrameChannel.open(channels.iterator());
    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromAdapter(adapter, null, false),
        FrameTestUtil.readRowsFromFrameChannel(concatChannel, FrameReader.create(adapter.getRowSignature()))
    );
  }
}
