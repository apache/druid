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

package org.apache.druid.jdbc.http;

import java.io.Closeable;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

/**
 * Streaming iterator for SQL query results in "array" format. Closeable and must be closed when done.
 * Does not implement {@link Iterator}, so we can throw {@link SQLException} from {@link #hasNext()}
 * and {@link #next}.
 */
public interface QueryResultsIterator extends Closeable
{
  boolean hasNext() throws SQLException;

  Object[] next() throws SQLException;

  List<ColumnMetadata> getColumns();
}
