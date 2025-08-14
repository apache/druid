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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.input.table.DataServerRequestDescriptor;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.aggregation.MetricManipulationFn;

import java.util.function.Function;

/**
 * Object for issuing native queries to data servers. This is created by a {@link DataServerQueryHandlerFactory},
 * and is used when MSQ is querying realtime data. It is required because realtime tasks are not currently able to
 * execute MSQ logic themselves.
 */
public interface DataServerQueryHandler
{
  /**
   * Issues a query to the server and segments that were specified by the {@link DataServerRequestDescriptor}
   * originally passed to {@link DataServerQueryHandlerFactory#createDataServerQueryHandler} when this instance
   * was created.
   *
   * The query datasource is updated to refer to the specific segments from
   * {@link DataServerRequestDescriptor#getSegments()}.
   *
   * Also applies {@link QueryToolChest#makePreComputeManipulatorFn(Query, MetricManipulationFn)} and reports channel
   * metrics on the returned results.
   *
   * @param query           query to run
   * @param mappingFunction function to apply to results
   * @param closer          will register query canceler with this closer
   * @param <QueryType>     result return type for the query from the data server
   * @param <RowType>       type of the result rows after parsing from QueryType object
   */
  <RowType, QueryType> ListenableFuture<DataServerQueryResult<RowType>> fetchRowsFromDataServer(
      Query<QueryType> query,
      Function<Sequence<QueryType>, Sequence<RowType>> mappingFunction,
      Closer closer
  );
}
