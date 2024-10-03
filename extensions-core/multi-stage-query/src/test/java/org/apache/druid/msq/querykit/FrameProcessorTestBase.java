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

package org.apache.druid.msq.querykit;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.processor.FrameProcessorExecutor;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.StagePartition;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class FrameProcessorTestBase extends InitializedNullHandlingTest
{
  protected static final StagePartition STAGE_PARTITION = new StagePartition(new StageId("q", 0), 0);

  private ListeningExecutorService innerExec;
  protected FrameProcessorExecutor exec;

  @Before
  public void setUp()
  {
    innerExec = MoreExecutors.listeningDecorator(Execs.singleThreaded("test-exec"));
    exec = new FrameProcessorExecutor(innerExec);
  }

  @After
  public void tearDown() throws Exception
  {
    innerExec.shutdownNow();
    innerExec.awaitTermination(10, TimeUnit.MINUTES);
  }

  protected ReadableInput makeChannelFromCursorFactory(
      final CursorFactory cursorFactory,
      final List<KeyColumn> keyColumns,
      int rowsPerInputFrame
  ) throws IOException
  {
    // Create a single, sorted frame.
    final FrameSequenceBuilder singleFrameBuilder =
        FrameSequenceBuilder.fromCursorFactory(cursorFactory)
                            .frameType(FrameType.ROW_BASED)
                            .maxRowsPerFrame(Integer.MAX_VALUE)
                            .sortBy(keyColumns);

    final RowSignature signature = singleFrameBuilder.signature();
    final Frame frame = Iterables.getOnlyElement(singleFrameBuilder.frames().toList());

    // Split it up into frames that match rowsPerFrame. Set max size enough to hold all rows we might ever want to use.
    final BlockingQueueFrameChannel channel = new BlockingQueueFrameChannel(10_000);

    final FrameReader frameReader = FrameReader.create(signature);

    final FrameSequenceBuilder frameSequenceBuilder =
        FrameSequenceBuilder.fromCursorFactory(frameReader.makeCursorFactory(frame))
                            .frameType(FrameType.ROW_BASED)
                            .maxRowsPerFrame(rowsPerInputFrame);

    final Sequence<Frame> frames = frameSequenceBuilder.frames();
    frames.forEach(
        f -> {
          try {
            channel.writable().write(f);
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
    );

    channel.writable().close();
    return ReadableInput.channel(channel.readable(), FrameReader.create(signature), STAGE_PARTITION);
  }
}
