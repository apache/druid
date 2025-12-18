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
import org.apache.druid.segment.incremental.ThrownAwayReason;

import javax.annotation.Nullable;
import java.util.function.Predicate;

/**
 * A filter for input rows during ingestion that can report the reason for rejection or null for acceptance.
 * This is similar to {@link Predicate} but returns the rejection reason instead of just a boolean.
 */
@FunctionalInterface
public interface RowFilter
{
  /**
   * Tests whether the given row should be accepted.
   *
   * @param row the input row to test
   * @return null if the row should be accepted, or the {@link ThrownAwayReason} if the row should be rejected
   */
  @Nullable
  ThrownAwayReason test(InputRow row);

  /**
   * Creates a {@link RowFilter} from a Predicate. When the predicate returns false,
   * the rejection reason will be {@link ThrownAwayReason#FILTERED}.
   */
  static RowFilter fromPredicate(Predicate<InputRow> predicate)
  {
    return row -> predicate.test(row) ? null : ThrownAwayReason.FILTERED;
  }

  /**
   * Fully-permissive {@link RowFilter} used mainly for tests.
   */
  static RowFilter allow()
  {
    return row -> null;
  }

  /**
   * Combines this filter with another filter. A row is rejected if either filter rejects it.
   * The rejection reason from the first rejecting filter (this filter first) is returned.
   */
  default RowFilter and(RowFilter other)
  {
    return row -> {
      ThrownAwayReason reason = this.test(row);
      if (reason != null) {
        return reason;
      }
      return other.test(row);
    };
  }
}

