/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.indexing.kafka;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.Map;

final class SetEndOffsetsResult
{
  private final boolean ok;
  @Nullable
  private final Map<Integer, Long> updatedEndOffsets;
  @Nullable
  private final String errorMessage;

  static SetEndOffsetsResult ok(Map<Integer, Long> updatedEndOffsets)
  {
    return new SetEndOffsetsResult(true, updatedEndOffsets, null);
  }

  static SetEndOffsetsResult fail(String errorMessage)
  {
    return new SetEndOffsetsResult(false, null, errorMessage);
  }

  private SetEndOffsetsResult(boolean ok, @Nullable Map<Integer, Long> updatedEndOffsets, @Nullable String errorMessage)
  {
    if (ok) {
      Preconditions.checkNotNull(updatedEndOffsets, "updatedEndOffsets");
    } else {
      Preconditions.checkNotNull(errorMessage, "errorMessage");
    }
    this.ok = ok;
    this.updatedEndOffsets = updatedEndOffsets;
    this.errorMessage = errorMessage;
  }

  boolean isOk()
  {
    return ok;
  }

  @Nullable
  Map<Integer, Long> getUpdatedEndOffsets()
  {
    return updatedEndOffsets;
  }

  @Nullable
  String getErrorMessage()
  {
    return errorMessage;
  }
}
