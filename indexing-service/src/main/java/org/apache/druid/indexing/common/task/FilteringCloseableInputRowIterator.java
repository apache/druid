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
import org.apache.druid.segment.incremental.ThrownAwayReason;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * An {@link InputRow} iterator used by ingestion {@link Task}s. It can filter out rows which do not satisfy the given
 * {@link RowFilter} or throw {@link ParseException} while parsing them. The relevant metric should be counted whenever
 * it filters out rows based on the filter. ParseException handling is delegatged to {@link ParseExceptionHandler}.
 */
public class FilteringCloseableInputRowIterator implements CloseableIterator<InputRow>
{
  private final CloseableIterator<InputRow> delegate;
  private final RowFilter rowFilter;
  private final RowIngestionMeters rowIngestionMeters;
  private final ParseExceptionHandler parseExceptionHandler;

  private InputRow next;

  public FilteringCloseableInputRowIterator(
      CloseableIterator<InputRow> delegate,
      RowFilter rowFilter,
      RowIngestionMeters rowIngestionMeters,
      ParseExceptionHandler parseExceptionHandler
  )
  {
    this.delegate = delegate;
    this.rowFilter = rowFilter;
    this.rowIngestionMeters = rowIngestionMeters;
    this.parseExceptionHandler = parseExceptionHandler;
  }

  @Override
  public boolean hasNext()
  {
    while (true) {
      try {
        // delegate.hasNext() can throw ParseException, since some types of delegating iterators will call next on
        // their underlying iterator
        while (next == null && delegate.hasNext()) {
          // delegate.next() can throw ParseException
          final InputRow row = delegate.next();
          // rowFilter.test() can throw ParseException, returns null if accepted, or reason if rejected
          final ThrownAwayReason rejectionReason = rowFilter.test(row);
          if (rejectionReason == null) {
            next = row;
          } else {
            rowIngestionMeters.incrementThrownAway(rejectionReason);
          }
        }
        break;
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
