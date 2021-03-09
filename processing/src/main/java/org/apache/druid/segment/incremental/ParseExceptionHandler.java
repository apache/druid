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

package org.apache.druid.segment.incremental;

import org.apache.druid.com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.utils.CircularBuffer;

import javax.annotation.Nullable;

/**
 * A handler for {@link ParseException}s thrown during ingestion. Based on the given configuration, this handler can
 *
 * - log ParseExceptions.
 * - keep most recent N ParseExceptions in memory.
 * - throw a RuntimeException when it sees more ParseExceptions than {@link #maxAllowedParseExceptions}.
 *
 * No matter what the handler does, the relevant metric should be updated first.
 */
public class ParseExceptionHandler
{
  private static final Logger LOG = new Logger(ParseExceptionHandler.class);

  private final RowIngestionMeters rowIngestionMeters;
  private final boolean logParseExceptions;
  private final int maxAllowedParseExceptions;
  @Nullable
  private final CircularBuffer<ParseException> savedParseExceptions;

  public ParseExceptionHandler(
      RowIngestionMeters rowIngestionMeters,
      boolean logParseExceptions,
      int maxAllowedParseExceptions,
      int maxSavedParseExceptions
  )
  {
    this.rowIngestionMeters = Preconditions.checkNotNull(rowIngestionMeters, "rowIngestionMeters");
    this.logParseExceptions = logParseExceptions;
    this.maxAllowedParseExceptions = maxAllowedParseExceptions;
    if (maxSavedParseExceptions > 0) {
      this.savedParseExceptions = new CircularBuffer<>(maxSavedParseExceptions);
    } else {
      this.savedParseExceptions = null;
    }
  }

  public void handle(@Nullable ParseException e)
  {
    if (e == null) {
      return;
    }
    if (e.isFromPartiallyValidRow()) {
      rowIngestionMeters.incrementProcessedWithError();
    } else {
      rowIngestionMeters.incrementUnparseable();
    }

    if (logParseExceptions) {
      LOG.error(e, "Encountered parse exception");
    }

    if (savedParseExceptions != null) {
      savedParseExceptions.add(e);
    }

    if (rowIngestionMeters.getUnparseable() + rowIngestionMeters.getProcessedWithError() > maxAllowedParseExceptions) {
      throw new RE("Max parse exceptions[%s] exceeded", maxAllowedParseExceptions);
    }
  }

  @Nullable
  public CircularBuffer<ParseException> getSavedParseExceptions()
  {
    return savedParseExceptions;
  }
}
