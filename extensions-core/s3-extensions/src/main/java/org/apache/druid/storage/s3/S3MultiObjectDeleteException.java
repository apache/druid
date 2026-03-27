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

package org.apache.druid.storage.s3;

import software.amazon.awssdk.services.s3.model.S3Error;

import java.util.List;

/**
 * Thrown when a {@code DeleteObjects} request returns one or more per-key errors in its response body
 * and all retries are exhausted. Unlike {@code S3Exception}, these errors are not thrown by the SDK –
 * they are embedded in a nominally successful response and must be checked explicitly via
 * {@code DeleteObjectsResponse#hasErrors()}.
 *
 * Callers like {@link S3Utils#deleteBucketKeys} retry only the failed keys on each attempt; this
 * exception is thrown when partial failures persist after all retries.
 */
public class S3MultiObjectDeleteException extends RuntimeException
{
  private final List<S3Error> errors;

  public S3MultiObjectDeleteException(List<S3Error> errors)
  {
    super(buildMessage(errors));
    this.errors = errors;
  }

  public List<S3Error> getErrors()
  {
    return errors;
  }

  private static String buildMessage(List<S3Error> errors)
  {
    StringBuilder sb = new StringBuilder("S3 multi-object delete had ")
        .append(errors.size())
        .append(" error(s):");
    for (S3Error error : errors) {
      sb.append("\n  key=[").append(error.key())
        .append("], code=[").append(error.code())
        .append("], message=[").append(error.message()).append("]");
    }
    return sb.toString();
  }
}
