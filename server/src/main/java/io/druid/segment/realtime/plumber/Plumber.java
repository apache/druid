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

package io.druid.segment.realtime.plumber;

import com.google.common.base.Supplier;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.segment.incremental.IncrementalIndexAddResult;
import io.druid.segment.incremental.IndexSizeExceededException;

public interface Plumber
{
  IncrementalIndexAddResult THROWAWAY = new IncrementalIndexAddResult(-1, -1, null, "row too late");
  IncrementalIndexAddResult NOT_WRITABLE = new IncrementalIndexAddResult(-1, -1, null, "not writable");
  IncrementalIndexAddResult DUPLICATE = new IncrementalIndexAddResult(-2, -1, null, "duplicate row");

  /**
   * Perform any initial setup. Should be called before using any other methods, and should be paired
   * with a corresponding call to {@link #finishJob}.
   *
   * @return the metadata of the "newest" segment that might have previously been persisted
   */
  Object startJob();

  /**
   * @param row               the row to insert
   * @param committerSupplier supplier of a committer associated with all data that has been added, including this row
   *
   * @return IncrementalIndexAddResult whose rowCount
   * - positive numbers indicate how many summarized rows exist in the index for that timestamp,
   * -1 means a row was thrown away because it was too late
   * -2 means a row was thrown away because it is duplicate
   */
  IncrementalIndexAddResult add(InputRow row, Supplier<Committer> committerSupplier) throws IndexSizeExceededException;

  <T> QueryRunner<T> getQueryRunner(Query<T> query);

  /**
   * Persist any in-memory indexed data to durable storage. This may be only somewhat durable, e.g. the
   * machine's local disk.
   *
   * @param committer committer to use after persisting data
   */
  void persist(Committer committer);

  /**
   * Perform any final processing and clean up after ourselves. Should be called after all data has been
   * fed into sinks and persisted.
   */
  void finishJob();
}
