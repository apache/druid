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
import org.apache.druid.segment.incremental.InputRowFilterResult;

import java.util.function.Predicate;

/**
 * A filter for input rows during ingestion that returns a {@link InputRowFilterResult} per row.
 * This is similar to {@link Predicate} but returns an {@link InputRowFilterResult} instead of a boolean.
 */
@FunctionalInterface
public interface InputRowFilter
{
  /**
   * Tests whether the given row should be accepted.
   *
   * @return {@link InputRowFilterResult#ACCEPTED} if the row should be accepted, or another {@link InputRowFilterResult} value if the row should be rejected
   */
  InputRowFilterResult test(InputRow row);

  /**
   * Creates a {@link InputRowFilter} from a {@link Predicate}.
   * Callers wishing to return custom rejection reason logic should implement their own {@link InputRowFilter} directly.
   */
  static InputRowFilter fromPredicate(Predicate<InputRow> predicate)
  {
    return row -> predicate.test(row) ? InputRowFilterResult.ACCEPTED : InputRowFilterResult.FILTERED;
  }

  /**
   * Fully-permissive {@link InputRowFilter} used mainly for tests.
   */
  static InputRowFilter allowAll()
  {
    return row -> InputRowFilterResult.ACCEPTED;
  }

  /**
   * Combines this filter with another filter. A row is rejected if either filter rejects it.
   * The rejection reason from the first rejecting filter (this filter first) is returned.
   */
  default InputRowFilter and(InputRowFilter other)
  {
    return row -> {
      InputRowFilterResult result = this.test(row);
      if (result.isRejected()) {
        return result;
      }
      return other.test(row);
    };
  }
}

