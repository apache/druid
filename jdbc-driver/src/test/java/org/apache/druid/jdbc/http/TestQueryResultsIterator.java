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

import java.util.Iterator;
import java.util.List;

/**
 * Stands in for a server response, so that tests need not go through QueryResultsIteratorImpl.
 */
public class TestQueryResultsIterator implements QueryResultsIterator
{
  private final List<ColumnMetadata> columns;
  private final Iterator<Object[]> rows;

  private boolean closed = false;

  public TestQueryResultsIterator(final List<ColumnMetadata> columns, final List<Object[]> rows)
  {
    this.columns = columns;
    this.rows = rows.iterator();
  }

  public static TestQueryResultsIterator empty(final List<ColumnMetadata> columns)
  {
    return new TestQueryResultsIterator(columns, List.of());
  }

  @Override
  public boolean hasNext()
  {
    return rows.hasNext();
  }

  @Override
  public Object[] next()
  {
    return rows.next();
  }

  @Override
  public List<ColumnMetadata> getColumns()
  {
    return columns;
  }

  @Override
  public void close()
  {
    closed = true;
  }

  public boolean isClosed()
  {
    return closed;
  }
}
