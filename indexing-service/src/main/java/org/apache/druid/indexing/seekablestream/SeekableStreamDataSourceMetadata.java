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

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.java.util.common.IAE;

import java.util.Objects;

public abstract class SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType>
    implements DataSourceMetadata
{
  private final SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> seekableStreamSequenceNumbers;

  public SeekableStreamDataSourceMetadata(
      SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> seekableStreamSequenceNumbers
  )
  {
    this.seekableStreamSequenceNumbers = seekableStreamSequenceNumbers;
  }

  @JsonProperty("partitions")
  public SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> getSeekableStreamSequenceNumbers()
  {
    return seekableStreamSequenceNumbers;
  }

  @Override
  public boolean isValidStart()
  {
    return true;
  }

  @Override
  public boolean matches(DataSourceMetadata other)
  {
    if (!getClass().equals(other.getClass())) {
      return false;
    }

    return plus(other).equals(other.plus(this));
  }

  @Override
  public DataSourceMetadata plus(DataSourceMetadata other)
  {
    if (this.getClass() != other.getClass()) {
      throw new IAE(
          "Expected instance of %s, got %s",
          this.getClass().getName(),
          other.getClass().getName()
      );
    }

    //noinspection unchecked
    final SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType> that =
        (SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType>) other;

    return createConcreteDataSourceMetaData(seekableStreamSequenceNumbers.plus(that.seekableStreamSequenceNumbers));
  }

  @Override
  public DataSourceMetadata minus(DataSourceMetadata other)
  {
    if (this.getClass() != other.getClass()) {
      throw new IAE(
          "Expected instance of %s, got %s",
          this.getClass().getName(),
          other.getClass().getName()
      );
    }

    //noinspection unchecked
    final SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType> that =
        (SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType>) other;

    return createConcreteDataSourceMetaData(seekableStreamSequenceNumbers.minus(that.seekableStreamSequenceNumbers));
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }
    SeekableStreamDataSourceMetadata that = (SeekableStreamDataSourceMetadata) o;
    return Objects.equals(getSeekableStreamSequenceNumbers(), that.getSeekableStreamSequenceNumbers());
  }

  @Override
  public int hashCode()
  {
    return seekableStreamSequenceNumbers.hashCode();
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "SeekableStreamStartSequenceNumbers=" + getSeekableStreamSequenceNumbers() +
           '}';
  }

  protected abstract SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType> createConcreteDataSourceMetaData(
      SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> seekableStreamSequenceNumbers
  );
}
