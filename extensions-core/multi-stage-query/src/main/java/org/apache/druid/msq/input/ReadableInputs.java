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

package org.apache.druid.msq.input;

import com.google.common.base.Preconditions;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * Iterable sequence of {@link ReadableInput}. Returned by {@link InputSliceReader#attach}.
 *
 * Inputs in the sequence are either all segments, or all channels. Segments and channels are not mixed.
 */
public class ReadableInputs implements Iterable<ReadableInput>
{
  private final Iterable<ReadableInput> iterable;

  @Nullable
  private final FrameReader frameReader;

  private ReadableInputs(Iterable<ReadableInput> iterable, @Nullable FrameReader frameReader)
  {
    this.iterable = Preconditions.checkNotNull(iterable, "iterable");
    this.frameReader = frameReader;
  }

  /**
   * Create a sequence of channel-based {@link ReadableInput}.
   */
  public static ReadableInputs channels(final Iterable<ReadableInput> iterable, FrameReader frameReader)
  {
    return new ReadableInputs(iterable, Preconditions.checkNotNull(frameReader, "frameReader"));
  }

  /**
   * Create a sequence of segment-based {@link ReadableInput}.
   */
  public static ReadableInputs segments(final Iterable<ReadableInput> iterable)
  {
    return new ReadableInputs(iterable, null);
  }

  /**
   * Returns the {@link ReadableInput} as an Iterator.
   */
  @Override
  public Iterator<ReadableInput> iterator()
  {
    return iterable.iterator();
  }

  /**
   * Return the frame reader for channel-based inputs. Throws {@link IllegalStateException} if this instance represents
   * segments rather than channels.
   */
  public FrameReader frameReader()
  {
    if (frameReader == null) {
      throw new ISE("No frame reader; check hasChannels() first");
    }

    return frameReader;
  }

  /**
   * Whether this instance represents channels. If false, the instance represents segments.
   */
  public boolean isChannelBased()
  {
    return frameReader != null;
  }
}
