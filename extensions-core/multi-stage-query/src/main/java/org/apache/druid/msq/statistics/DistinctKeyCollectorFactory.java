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

package org.apache.druid.msq.statistics;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import it.unimi.dsi.fastutil.objects.Object2LongRBTreeMap;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.RowKey;

import java.io.IOException;
import java.util.Comparator;
import java.util.stream.Collectors;

public class DistinctKeyCollectorFactory implements KeyCollectorFactory<DistinctKeyCollector, DistinctKeySnapshot>
{
  private final Comparator<RowKey> comparator;

  private DistinctKeyCollectorFactory(Comparator<RowKey> comparator)
  {
    this.comparator = comparator;
  }

  static DistinctKeyCollectorFactory create(final ClusterBy clusterBy)
  {
    return new DistinctKeyCollectorFactory(clusterBy.keyComparator());
  }

  @Override
  public DistinctKeyCollector newKeyCollector()
  {
    return new DistinctKeyCollector(comparator);
  }

  @Override
  public JsonDeserializer<DistinctKeySnapshot> snapshotDeserializer()
  {
    return new JsonDeserializer<DistinctKeySnapshot>()
    {
      @Override
      public DistinctKeySnapshot deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException
      {
        return jp.readValueAs(DistinctKeySnapshot.class);
      }
    };
  }

  @Override
  public DistinctKeySnapshot toSnapshot(final DistinctKeyCollector collector)
  {
    return new DistinctKeySnapshot(
        collector.getRetainedKeys()
                 .entrySet()
                 .stream()
                 .map(entry -> new SerializablePair<>(entry.getKey(), entry.getValue()))
                 .collect(Collectors.toList()),
        collector.getSpaceReductionFactor()
    );
  }

  @Override
  public DistinctKeyCollector fromSnapshot(final DistinctKeySnapshot snapshot)
  {
    final Object2LongRBTreeMap<RowKey> retainedKeys = new Object2LongRBTreeMap<>(comparator);
    retainedKeys.putAll(snapshot.getKeysAsMap());
    return new DistinctKeyCollector(comparator, retainedKeys, snapshot.getSpaceReductionFactor());
  }
}
