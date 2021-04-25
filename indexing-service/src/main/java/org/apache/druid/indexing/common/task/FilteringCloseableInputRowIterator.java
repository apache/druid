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

package org.apache.druid.indexing.common.task;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

/**
 * An {@link InputRow} iterator used by ingestion {@link Task}s. It can filter out rows which do not satisfy the given
 * {@link #filter} or throw {@link ParseException} while parsing them. The relevant metric should be counted whenever
 * it filters out rows based on the filter. ParseException handling is delegatged to {@link ParseExceptionHandler}.
 */
public class FilteringCloseableInputRowIterator implements CloseableIterator<InputRow>
{
  private final CloseableIterator<InputRow> delegate;
  private final Predicate<InputRow> filter;
  private final RowIngestionMeters rowIngestionMeters;
  private final ParseExceptionHandler parseExceptionHandler;

  private InputRow next;

  public FilteringCloseableInputRowIterator(
      CloseableIterator<InputRow> delegate,
      Predicate<InputRow> filter,
      RowIngestionMeters rowIngestionMeters,
      ParseExceptionHandler parseExceptionHandler
  )
  {
    this.delegate = delegate;
    this.filter = filter;
    this.rowIngestionMeters = rowIngestionMeters;
    this.parseExceptionHandler = parseExceptionHandler;
  }

  @Override
  public boolean hasNext()
  {
    while (next == null && delegate.hasNext()) {
      try {
        // delegate.next() can throw ParseException
        final InputRow row = delegate.next();
        // filter.test() can throw ParseException
        if (filter.test(row)) {
          next = row;
        } else {
          rowIngestionMeters.incrementThrownAway();
        }
      }
      catch (ParseException e) {
        parseExceptionHandler.handle(e);
      }
    }
    return next != null;
  }

  @Override
  public InputRow next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final InputRow row = next;
    next = null;
    return row;
  }

  @Override
  public void close() throws IOException
  {
    delegate.close();
  }
}
