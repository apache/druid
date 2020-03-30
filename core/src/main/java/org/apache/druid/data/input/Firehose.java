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

package org.apache.druid.data.input;

import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.java.util.common.parsers.ParseException;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;

/**
 * This is an interface that holds onto the stream of incoming data.  Realtime data ingestion is built around this
 * abstraction.
 *
 * This object acts a lot like an Iterator, but it doesn't extend the Iterator interface because it extends
 * Closeable and it is very important that the {@link #close()} method doesn't get forgotten, which is easy to do if
 * this gets passed around as an Iterator. Note that {@link #close()} doesn't cut the stream of rows for Firehose users
 * immediately, but rather stops the supply of new rows into internal buffers. {@link #hasMore()} and {@link #nextRow()}
 * are expected to operate for some time after (or concurrently with) {@link #close()} until the buffered events (if
 * any) run out.
 *
 * Concurrency:
 * The two methods {@link #hasMore()} and {@link #nextRow()} are all called from the same thread.
 * {@link #close()} might be called concurrently from a thread different from the thread calling {@link #hasMore()}
 * and {@link #nextRow()}.
 * </p>
 */
@ExtensionPoint
public interface Firehose extends Closeable
{
  /**
   * Returns whether there are more rows to process.  This is used to indicate that another item is immediately
   * available via {@link #nextRow()}.  Thus, if the stream is still available but there are no new messages on it, this
   * call should block until a new message is available.
   *
   * If something happens such that the stream is no longer available, this should return false.
   *
   * @return true if and when there is another row available, false if the stream has dried up
   */
  boolean hasMore() throws IOException;

  /**
   * The next row available.  Should only be called if hasMore returns true.
   * The return value can be null which means the caller must skip this row.
   *
   * @return The next row
   */
  @Nullable
  InputRow nextRow() throws IOException;

  /**
   * Returns an {@link InputRowListPlusRawValues} object containing the InputRow plus the raw, unparsed data corresponding to
   * the next row available. Used in the sampler to provide the caller with information to assist in configuring a parse
   * spec. If a ParseException is thrown by the parser, it should be caught and returned in the InputRowListPlusRawValues so
   * we will be able to provide information on the raw row which failed to be parsed. Should only be called if hasMore
   * returns true.
   *
   * @return an InputRowListPlusRawValues which may contain any of: an InputRow, map of the raw data, or a ParseException
   */
  @Deprecated
  default InputRowListPlusRawValues nextRowWithRaw() throws IOException
  {
    try {
      return InputRowListPlusRawValues.of(nextRow(), null);
    }
    catch (ParseException e) {
      return InputRowListPlusRawValues.of(null, e);
    }
  }

  /**
   * Closes the "ingestion side" of the Firehose, potentially concurrently with calls to {@link #hasMore()} and {@link
   * #nextRow()} being made from a different thread. {@link #hasMore()} and {@link #nextRow()}
   * continue to work after close(), but since the ingestion side is closed rows will eventually run out.
   */
  @Override
  void close() throws IOException;
}
