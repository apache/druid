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

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.input.table.RichSegmentDescriptor;
import org.apache.druid.query.Query;

import java.io.IOException;
import java.util.function.Function;

/**
 * Class responsible for querying dataservers and retriving results for a given query. Also queries the coordinator
 * to check if a segment has been handed off.
 */
public interface LoadedSegmentDataProvider
{
  /**
   * Queries a data server and returns a {@link Yielder} for the results, retrying if needed. If a dataserver indicates
   * that the segment was not found, checks with the coordinator to see if the segment was handed off.
   * - If the segment was handed off, returns with a {@link DataServerQueryStatus#HANDOFF} status.
   * - If the segment was not handed off, retries with the known list of servers and throws an exception if the retry
   * count is exceeded.
   *
   * @param <QueryType> result return type for the query from the data server
   * @param <RowType> type of the result rows after parsing from QueryType object
   */
  <RowType, QueryType> Pair<DataServerQueryStatus, Yielder<RowType>> fetchRowsFromDataServer(
      Query<QueryType> query,
      RichSegmentDescriptor segmentDescriptor,
      Function<Sequence<QueryType>, Sequence<RowType>> mappingFunction,
      Class<QueryType> queryResultType,
      Closer closer
  ) throws IOException;

  /**
   * Represents the status of fetching a segment from a data server
   */
  enum DataServerQueryStatus
  {
    /**
     * Segment was found on the data server and fetched successfully.
     */
    SUCCESS,
    /**
     * Segment was not found on the realtime server as it has been handed off to a historical. Only returned while
     * querying a realtime server.
     */
    HANDOFF
  }
}
