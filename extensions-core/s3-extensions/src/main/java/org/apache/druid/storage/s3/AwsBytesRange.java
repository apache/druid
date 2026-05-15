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

import javax.annotation.Nullable;

/**
 * Formats an HTTP {@code Range} header value for S3 byte-range requests.
 *
 * <p>Use {@link #of(long, long)} for a closed range ({@code bytes=start-end}) or
 * {@link #from(long)} for an open-ended range ({@code bytes=start-}).
 */
public class AwsBytesRange
{
  private final long start;
  @Nullable
  private final Long end;

  private AwsBytesRange(long start, @Nullable Long end)
  {
    this.start = start;
    this.end = end;
  }

  /**
   * Creates a closed byte range: {@code bytes=start-end}.
   */
  public static AwsBytesRange of(long start, long end)
  {
    return new AwsBytesRange(start, end);
  }

  /**
   * Creates an open-ended byte range: {@code bytes=start-}.
   */
  public static AwsBytesRange from(long start)
  {
    return new AwsBytesRange(start, null);
  }

  /**
   * Returns the formatted HTTP {@code Range} header value.
   */
  public String getBytesRange()
  {
    if (end == null) {
      return "bytes=" + start + "-";
    }
    return "bytes=" + start + "-" + end;
  }
}
