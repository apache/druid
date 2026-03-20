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

import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.FrameChannelSequence;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

import java.io.IOException;

/**
 * Listener that provides a {@link Sequence} of {@link RowsAndColumns}, via {@link #getSequence()}.
 */
public class SequenceQueryListener implements QueryListener
{
  private static final int DEFAULT_CAPACITY = 2;

  private final BlockingQueueFrameChannel channel;
  private volatile FrameReader frameReader;

  public SequenceQueryListener()
  {
    this(DEFAULT_CAPACITY);
  }

  public SequenceQueryListener(final int bufferSize)
  {
    this.channel = new BlockingQueueFrameChannel(bufferSize);
  }

  /**
   * Returns a {@link Sequence} of {@link RowsAndColumns} read from the channel. The sequence performs blocking
   * reads; it should be consumed from a thread that is allowed to block.
   */
  public Sequence<RowsAndColumns> getSequence()
  {
    return new FrameChannelSequence(channel.readable());
  }

  /**
   * Returns the {@link FrameReader} set by {@link #onResultsStart(FrameReader)}.
   */
  public FrameReader getFrameReader()
  {
    return frameReader;
  }

  @Override
  public boolean readResults()
  {
    return true;
  }

  @Override
  public void onResultsStart(final FrameReader frameReader)
  {
    this.frameReader = frameReader;
  }

  @Override
  public boolean onResultBatch(final RowsAndColumns rac)
  {
    try {
      final WritableFrameChannel writable = channel.writable();

      // Blocking write.
      FutureUtils.getUnchecked(writable.writabilityFuture(), false);
      writable.write(rac);
      return true;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onResultsComplete()
  {
    // Nothing to do. We'll close the writable channel in onQueryComplete.
  }

  @Override
  public void onQueryComplete(final MSQTaskReportPayload report)
  {
    try {
      final WritableFrameChannel writable = channel.writable();

      if (!writable.isClosed()) {
        if (!report.getStatus().getStatus().isSuccess()) {
          writable.fail(report.getStatus().getErrorReport().toDruidException());
        }

        writable.close();
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
