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

package org.apache.druid.indexing.kafka;

import javax.annotation.Nullable;
import java.util.List;

final class ShareGroupStagingCompletion
{
  private static final ShareGroupStagingCompletion STOP = new ShareGroupStagingCompletion(List.of(), null, true);

  private final List<ShareGroupAcquisitionRegistry.UploadAttempt> attempts;
  @Nullable
  private final Exception failure;
  private final boolean stop;

  private ShareGroupStagingCompletion(
      List<ShareGroupAcquisitionRegistry.UploadAttempt> attempts,
      @Nullable Exception failure,
      boolean stop
  )
  {
    this.attempts = List.copyOf(attempts);
    this.failure = failure;
    this.stop = stop;
  }

  static ShareGroupStagingCompletion success(List<ShareGroupAcquisitionRegistry.UploadAttempt> attempts)
  {
    return new ShareGroupStagingCompletion(attempts, null, false);
  }

  static ShareGroupStagingCompletion failure(
      List<ShareGroupAcquisitionRegistry.UploadAttempt> attempts,
      Exception failure
  )
  {
    return new ShareGroupStagingCompletion(attempts, failure, false);
  }

  static ShareGroupStagingCompletion stop()
  {
    return STOP;
  }

  List<ShareGroupAcquisitionRegistry.UploadAttempt> getAttempts()
  {
    return attempts;
  }

  boolean isSuccessful()
  {
    return failure == null && !stop;
  }

  boolean isStop()
  {
    return stop;
  }
}
