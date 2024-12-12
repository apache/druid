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

package org.apache.druid.storage.google.output;

import java.util.Objects;

public class GoogleInputRange
{
  private final long start;
  private final long size;
  private final String bucket;
  private final String path;

  public GoogleInputRange(long start, long size, String bucket, String path)
  {
    this.start = start;
    this.size = size;
    this.bucket = bucket;
    this.path = path;
  }

  public long getStart()
  {
    return start;
  }

  public long getSize()
  {
    return size;
  }

  public String getBucket()
  {
    return bucket;
  }

  public String getPath()
  {
    return path;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GoogleInputRange that = (GoogleInputRange) o;
    return start == that.start
           && size == that.size
           && Objects.equals(bucket, that.bucket)
           && Objects.equals(path, that.path);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(start, size, bucket, path);
  }

  @Override
  public String toString()
  {
    return "GoogleInputRange{" +
           "start=" + start +
           ", size=" + size +
           ", bucket='" + bucket + '\'' +
           ", path='" + path + '\'' +
           '}';
  }
}
