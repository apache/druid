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

package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.java.util.common.IAE;

import java.util.Map;
import java.util.Objects;

public class KinesisDataSourceMetadata implements DataSourceMetadata
{
  private final KinesisPartitions kinesisPartitions;

  @JsonCreator
  public KinesisDataSourceMetadata(
      @JsonProperty("partitions") KinesisPartitions kinesisPartitions
  )
  {
    this.kinesisPartitions = kinesisPartitions;
  }

  @JsonProperty("partitions")
  public KinesisPartitions getKinesisPartitions()
  {
    return kinesisPartitions;
  }

  @Override
  public boolean isValidStart()
  {
    return true;
  }

  @Override
  public boolean matches(DataSourceMetadata other)
  {
    if (getClass() != other.getClass()) {
      return false;
    }

    return plus(other).equals(other.plus(this));
  }

  @Override
  public DataSourceMetadata plus(DataSourceMetadata other)
  {
    if (!(other instanceof KinesisDataSourceMetadata)) {
      throw new IAE(
          "Expected instance of %s, got %s",
          KinesisDataSourceMetadata.class.getCanonicalName(),
          other.getClass().getCanonicalName()
      );
    }

    final KinesisDataSourceMetadata that = (KinesisDataSourceMetadata) other;

    if (that.getKinesisPartitions().getStream().equals(kinesisPartitions.getStream())) {
      // Same topic, merge sequence numbers.
      final Map<String, String> newMap = Maps.newHashMap();

      for (Map.Entry<String, String> entry : kinesisPartitions.getPartitionSequenceNumberMap().entrySet()) {
        newMap.put(entry.getKey(), entry.getValue());
      }

      for (Map.Entry<String, String> entry : that.getKinesisPartitions().getPartitionSequenceNumberMap().entrySet()) {
        newMap.put(entry.getKey(), entry.getValue());
      }

      return new KinesisDataSourceMetadata(new KinesisPartitions(kinesisPartitions.getStream(), newMap));
    } else {
      // Different topic, prefer "other".
      return other;
    }
  }

  @Override
  public DataSourceMetadata minus(DataSourceMetadata other)
  {
    if (!(other instanceof KinesisDataSourceMetadata)) {
      throw new IAE(
          "Expected instance of %s, got %s",
          KinesisDataSourceMetadata.class.getCanonicalName(),
          other.getClass().getCanonicalName()
      );
    }

    final KinesisDataSourceMetadata that = (KinesisDataSourceMetadata) other;

    if (that.getKinesisPartitions().getStream().equals(kinesisPartitions.getStream())) {
      // Same stream, remove partitions present in "that" from "this"
      final Map<String, String> newMap = Maps.newHashMap();

      for (Map.Entry<String, String> entry : kinesisPartitions.getPartitionSequenceNumberMap().entrySet()) {
        if (!that.getKinesisPartitions().getPartitionSequenceNumberMap().containsKey(entry.getKey())) {
          newMap.put(entry.getKey(), entry.getValue());
        }
      }

      return new KinesisDataSourceMetadata(new KinesisPartitions(kinesisPartitions.getStream(), newMap));
    } else {
      // Different stream, prefer "this".
      return this;
    }
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
    KinesisDataSourceMetadata that = (KinesisDataSourceMetadata) o;
    return Objects.equals(kinesisPartitions, that.kinesisPartitions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(kinesisPartitions);
  }

  @Override
  public String toString()
  {
    return "KinesisDataSourceMetadata{" +
           "kinesisPartitions=" + kinesisPartitions +
           '}';
  }
}
