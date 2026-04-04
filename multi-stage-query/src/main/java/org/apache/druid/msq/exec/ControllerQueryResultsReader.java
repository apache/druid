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

package org.apache.druid.msq.exec;

import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.FrameProcessors;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Used by {@link ControllerImpl} to read query results and hand them to a {@link QueryListener}.
 */
public class ControllerQueryResultsReader implements FrameProcessor<Void>
{
  private static final Logger log = new Logger(ControllerQueryResultsReader.class);

  private final ReadableFrameChannel in;
  private final FrameReader frameReader;
  private final QueryListener queryListener;

  private boolean wroteResultsStart;

  ControllerQueryResultsReader(
      final ReadableFrameChannel in,
      final FrameReader frameReader,
      final QueryListener queryListener
  )
  {
    this.in = in;
    this.frameReader = frameReader;
    this.queryListener = queryListener;
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return Collections.singletonList(in);
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.emptyList();
  }

  @Override
  public ReturnOrAwait<Void> runIncrementally(final IntSet readableInputs)
  {
    if (readableInputs.isEmpty()) {
      return ReturnOrAwait.awaitAll(inputChannels().size());
    }

    if (!wroteResultsStart) {
      queryListener.onResultsStart(frameReader);
      wroteResultsStart = true;
    }

    // Read from query results channel, if it's open.
    if (in.isFinished()) {
      queryListener.onResultsComplete();
      return ReturnOrAwait.returnObject(null);
    } else {
      queryListener.onResultBatch(in.read());
      return ReturnOrAwait.awaitAll(inputChannels().size());
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels());
  }
}
