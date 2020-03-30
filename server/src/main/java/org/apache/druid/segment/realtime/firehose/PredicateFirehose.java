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

package org.apache.druid.segment.realtime.firehose;

import com.google.common.base.Predicate;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Provides a view on a firehose that only returns rows that match a certain predicate.
 * Not thread-safe.
 */
public class PredicateFirehose implements Firehose
{
  private static final Logger log = new Logger(PredicateFirehose.class);
  private static final int IGNORE_THRESHOLD = 5000;
  private long ignored = 0;

  private final Firehose firehose;
  private final Predicate<InputRow> predicate;

  @Nullable
  private InputRow savedInputRow = null;

  public PredicateFirehose(Firehose firehose, Predicate<InputRow> predicate)
  {
    this.firehose = firehose;
    this.predicate = predicate;
  }

  @Override
  public boolean hasMore() throws IOException
  {
    if (savedInputRow != null) {
      return true;
    }

    while (firehose.hasMore()) {
      final InputRow row = firehose.nextRow();
      if (predicate.apply(row)) {
        savedInputRow = row;
        return true;
      }
      // Do not silently discard the rows
      if (ignored % IGNORE_THRESHOLD == 0) {
        log.warn("[%,d] InputRow(s) ignored as they do not satisfy the predicate", ignored);
      }
      ignored++;
    }

    return false;
  }

  @Nullable
  @Override
  public InputRow nextRow()
  {
    final InputRow row = savedInputRow;
    savedInputRow = null;
    return row;
  }

  @Override
  public void close() throws IOException
  {
    firehose.close();
  }
}
