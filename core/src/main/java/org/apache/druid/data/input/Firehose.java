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
 * The three methods {@link #hasMore()}, {@link #nextRow()} and {@link #commit()} are all called from the same thread.
 * {@link #commit()}, however, returns a callback which will be called on another thread. {@link #close()} might be
 * called concurrently from a thread different from the thread calling {@link #hasMore()}, {@link #nextRow()} and {@link
 * #commit()}.
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
  boolean hasMore();

  /**
   * The next row available.  Should only be called if hasMore returns true.
   * The return value can be null which means the caller must skip this row.
   *
   * @return The next row
   */
  @Nullable
  InputRow nextRow();

  /**
   * Returns an InputRowPlusRaw object containing the InputRow plus the raw, unparsed data corresponding to the next row
   * available. Used in the sampler to provide the caller with information to assist in configuring a parse spec. If a
   * ParseException is thrown by the parser, it should be caught and returned in the InputRowPlusRaw so we will be able
   * to provide information on the raw row which failed to be parsed. Should only be called if hasMore returns true.
   *
   * @return an InputRowPlusRaw which may contain any of: an InputRow, the raw data, or a ParseException
   */
  default InputRowPlusRaw nextRowWithRaw()
  {
    try {
      return InputRowPlusRaw.of(nextRow(), null);
    }
    catch (ParseException e) {
      return InputRowPlusRaw.of(null, e);
    }
  }

  /**
   * Returns a runnable that will "commit" everything read up to the point at which commit() is called.  This is
   * often equivalent to everything that has been read since the last commit() call (or instantiation of the object),
   * but doesn't necessarily have to be.
   *
   * This method is called when the main processing loop starts to persist its current batch of things to process.
   * The returned runnable will be run when the current batch has been successfully persisted, there is usually
   * some time lag between when this method is called and when the runnable is run.  The Runnable is also run on
   * a separate thread so its operation should be thread-safe.
   *
   * The Runnable is essentially just a lambda/closure that is run() after data supplied by this instance has
   * been committed on the writer side of this interface protocol.
   * <p>
   * A simple implementation of this interface might do nothing when run() is called
   * (in which case the same do-nothing instance can be returned every time), or
   * a more complex implementation might clean up temporary resources that are no longer needed
   * because of InputRows delivered by prior calls to {@link #nextRow()}.
   * </p>
   */
  Runnable commit();

  /**
   * Closes the "ingestion side" of the Firehose, potentially concurrently with calls to {@link #hasMore()}, {@link
   * #nextRow()} and {@link #commit()} being made from a different thread. {@link #hasMore()} and {@link #nextRow()}
   * continue to work after close(), but since the ingestion side is closed rows will eventually run out.
   *
   * The effects of calling run() on the {@link Runnable} object returned from {@link #commit()} (in other words,
   * doing the commit) concurrently or after close() are unspecified: commit may not be performed silently (that is,
   * run() call completes without an Exception, but the commit is not actually done), or a error may result. Note that
   * {@link #commit()} method itself can be called concurrently with close(), but it doesn't make much sense, because
   * run() on the returned Runnable then can't be called.
   */
  @Override
  void close() throws IOException;
}
