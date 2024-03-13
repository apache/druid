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

import java.util.Objects;

public class GoogleStorageObjectMetadata
{
  final String bucket;
  final String name;
  final Long size;
  Long lastUpdateTimeMillis;

  public GoogleStorageObjectMetadata(
      final String bucket,
      final String name,
      final Long size,
      final Long lastUpdateTimeMillis
  )
  {
    this.bucket = bucket;
    this.name = name;
    this.size = size;
    this.lastUpdateTimeMillis = lastUpdateTimeMillis;
  }

  public void setLastUpdateTimeMillis(Long lastUpdateTimeMillis)
  {
    this.lastUpdateTimeMillis = lastUpdateTimeMillis;
  }


  public String getBucket()
  {
    return bucket;
  }

  public String getName()
  {
    return name;
  }

  public Long getSize()
  {
    return size;
  }

  public Long getLastUpdateTimeMillis()
  {
    return lastUpdateTimeMillis;
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
    GoogleStorageObjectMetadata that = (GoogleStorageObjectMetadata) o;
    return Objects.equals(bucket, that.bucket)
           && Objects.equals(name, that.name)
           && Objects.equals(size, that.size)
           && Objects.equals(lastUpdateTimeMillis, that.getLastUpdateTimeMillis());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(bucket, name, size, lastUpdateTimeMillis);
  }

  @Override
  public String toString()
  {
    return "GoogleStorageObjectMetadata{" +
           "bucket='" + bucket + '\'' +
           ", name='" + name + '\'' +
           ", size=" + size +
           ", lastUpdateTimeMillis=" + lastUpdateTimeMillis +
           '}';
  }
}
