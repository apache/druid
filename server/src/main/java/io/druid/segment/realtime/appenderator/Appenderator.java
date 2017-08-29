/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.realtime.appenderator;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.query.QuerySegmentWalker;
import io.druid.segment.incremental.IndexSizeExceededException;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;

/**
 * An Appenderator indexes data. It has some in-memory data and some persisted-on-disk data. It can serve queries on
 * both of those. It can also push data to deep storage. But, it does not decide which segments data should go into.
 * It also doesn't publish segments to the metadata store or monitor handoff; you have to do that yourself!
 * <p>
 * Any time you call one of the methods that adds, persists, or pushes data, you must provide a Committer, or a
 * Supplier of one, that represents all data you have given to the Appenderator so far. The Committer will be used when
 * that data has been persisted to disk.
 */
public interface Appenderator extends QuerySegmentWalker, Closeable
{
  /**
   * Return the name of the dataSource associated with this Appenderator.
   */
  String getDataSource();

  /**
   * Perform any initial setup. Should be called before using any other methods.
   *
   * @return currently persisted commit metadata
   */
  Object startJob();

  /**
   * Add a row. Must not be called concurrently from multiple threads.
   * <p>
   * If no pending segment exists for the provided identifier, a new one will be created.
   * <p>
   * This method may trigger a {@link #persistAll(Committer)} using the supplied Committer. If it does this, the
   * Committer is guaranteed to be *created* synchronously with the call to add, but will actually be used
   * asynchronously.
   * <p>
   * The add, clear, persist, persistAll, and push methods should all be called from the same thread to keep the
   * metadata committed by Committer in sync.
   *
   * @param identifier        the segment into which this row should be added
   * @param row               the row to add
   * @param committerSupplier supplier of a committer associated with all data that has been added, including this row
   *
   * @return positive number indicating how many summarized rows exist in this segment so far
   *
   * @throws IndexSizeExceededException  if this row cannot be added because it is too large
   * @throws SegmentNotWritableException if the requested segment is known, but has been closed
   */
  int add(SegmentIdentifier identifier, InputRow row, Supplier<Committer> committerSupplier)
      throws IndexSizeExceededException, SegmentNotWritableException;

  /**
   * Returns a list of all currently active segments.
   */
  List<SegmentIdentifier> getSegments();

  /**
   * Returns the number of rows in a particular pending segment.
   *
   * @param identifier segment to examine
   *
   * @return row count
   *
   * @throws IllegalStateException if the segment is unknown
   */
  int getRowCount(final SegmentIdentifier identifier);

  /**
   * Returns the number of total rows in this appenderator.
   *
   * @return total number of rows
   */
  int getTotalRowCount();

  /**
   * Drop all in-memory and on-disk data, and forget any previously-remembered commit metadata. This could be useful if,
   * for some reason, rows have been added that we do not actually want to hand off. Blocks until all data has been
   * cleared. This may take some time, since all pending persists must finish first.
   *
   * The add, clear, persist, persistAll, and push methods should all be called from the same thread to keep the
   * metadata committed by Committer in sync.
   */
  void clear() throws InterruptedException;

  /**
   * Drop all data associated with a particular pending segment. Unlike {@link #clear()}), any on-disk commit
   * metadata will remain unchanged. If there is no pending segment with this identifier, then this method will
   * do nothing.
   * <p>
   * You should not write to the dropped segment after calling "drop". If you need to drop all your data and
   * re-write it, consider {@link #clear()} instead.
   *
   * @param identifier the pending segment to drop
   *
   * @return future that resolves when data is dropped
   */
  ListenableFuture<?> drop(SegmentIdentifier identifier);

  /**
   * Persist any in-memory indexed data for segments of the given identifiers to durable storage. This may be only
   * somewhat durable, e.g. the machine's local disk. The Committer will be made synchronously with the call to
   * persist, but will actually be used asynchronously. Any metadata returned by the committer will be associated with
   * the data persisted to disk.
   * <p>
   * The add, clear, persist, persistAll, and push methods should all be called from the same thread to keep the
   * metadata committed by Committer in sync.
   *
   * @param identifiers segment identifiers to be persisted
   * @param committer   a committer associated with all data that has been added to segments of the given identifiers so
   *                   far
   *
   * @return future that resolves when all pending data to segments of the identifiers has been persisted, contains
   * commit metadata for this persist
   */
  ListenableFuture<Object> persist(Collection<SegmentIdentifier> identifiers, Committer committer);

  /**
   * Persist any in-memory indexed data to durable storage. This may be only somewhat durable, e.g. the
   * machine's local disk. The Committer will be made synchronously with the call to persistAll, but will actually
   * be used asynchronously. Any metadata returned by the committer will be associated with the data persisted to
   * disk.
   * <p>
   * The add, clear, persist, persistAll, and push methods should all be called from the same thread to keep the
   * metadata committed by Committer in sync.
   *
   * @param committer a committer associated with all data that has been added so far
   *
   * @return future that resolves when all pending data has been persisted, contains commit metadata for this persist
   */
  default ListenableFuture<Object> persistAll(Committer committer)
  {
    return persist(getSegments(), committer);
  }

  /**
   * Merge and push particular segments to deep storage. This will trigger an implicit
   * {@link #persist(Collection, Committer)} using the provided Committer.
   * <p>
   * After this method is called, you cannot add new data to any segments that were previously under construction.
   * <p>
   * The add, clear, persist, persistAll, and push methods should all be called from the same thread to keep the
   * metadata committed by Committer in sync.
   *
   * @param identifiers list of segments to push
   * @param committer   a committer associated with all data that has been added so far
   *
   * @return future that resolves when all segments have been pushed. The segment list will be the list of segments
   * that have been pushed and the commit metadata from the Committer.
   */
  ListenableFuture<SegmentsAndMetadata> push(Collection<SegmentIdentifier> identifiers, Committer committer);

  /**
   * Stop any currently-running processing and clean up after ourselves. This will not remove any on-disk persisted
   * data, but it will drop any data that has not yet been persisted.
   */
  @Override
  void close();
}
