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

package org.apache.druid.storage.google;

import com.google.common.io.ByteSource;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class GoogleByteSource extends ByteSource
{
  private final GoogleStorage storage;
  private final String bucket;
  private final String path;

  public GoogleByteSource(final GoogleStorage storage, final String bucket, final String path)
  {
    this.storage = storage;
    this.bucket = bucket;
    this.path = path;
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
  public InputStream openStream() throws IOException
  {
    return storage.getInputStream(bucket, path);
  }

  public InputStream openStream(long start) throws IOException
  {
    return storage.getInputStream(bucket, path, start);
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
    GoogleByteSource that = (GoogleByteSource) o;
    return Objects.equals(bucket, that.bucket) &&
           Objects.equals(path, that.path);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(bucket, path);
  }
}
