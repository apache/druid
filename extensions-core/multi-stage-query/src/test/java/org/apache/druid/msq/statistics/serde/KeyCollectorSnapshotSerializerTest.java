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

package org.apache.druid.msq.statistics.serde;

import com.google.common.collect.ImmutableList;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollectorImpl;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class KeyCollectorSnapshotSerializerTest extends InitializedNullHandlingTest
{
  private static final int MAX_BYTES = 1_000_000_000;
  private static final int MAX_BUCKETS = 10_000;

  private static final RowSignature SIGNATURE = RowSignature.builder()
                                                            .add("x", ColumnType.LONG)
                                                            .add("y", ColumnType.LONG)
                                                            .add("z", ColumnType.STRING)
                                                            .build();

  private static final ClusterBy CLUSTER_BY_XYZ_BUCKET_BY_X = new ClusterBy(
      ImmutableList.of(
          new KeyColumn("x", KeyOrder.ASCENDING),
          new KeyColumn("y", KeyOrder.ASCENDING),
          new KeyColumn("z", KeyOrder.ASCENDING)
      ),
      1
  );

  @Test
  public void testEmptyQuantilesSnapshot() throws IOException
  {
    ClusterByStatisticsCollectorImpl collector = makeCollector(false);
    ClusterByStatisticsSnapshot snapshot = collector.snapshot();

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    ClusterByStatisticsSnapshotSerde.serialize(byteArrayOutputStream, snapshot);
    final ByteBuffer serializedSnapshot = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    final ClusterByStatisticsSnapshot deserializedSnapshot = ClusterByStatisticsSnapshotSerde.deserialize(serializedSnapshot);

    Assert.assertEquals(snapshot, deserializedSnapshot);
  }

  @Test
  public void testEmptyDistinctKeySketchSnapshot() throws IOException
  {
    ClusterByStatisticsCollectorImpl collector = makeCollector(true);
    ClusterByStatisticsSnapshot snapshot = collector.snapshot();

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    ClusterByStatisticsSnapshotSerde.serialize(byteArrayOutputStream, snapshot);
    final ByteBuffer serializedSnapshot = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    final ClusterByStatisticsSnapshot deserializedSnapshot = ClusterByStatisticsSnapshotSerde.deserialize(serializedSnapshot);

    Assert.assertEquals(snapshot, deserializedSnapshot);
  }

  private ClusterByStatisticsCollectorImpl makeCollector(final boolean aggregate)
  {
    return (ClusterByStatisticsCollectorImpl) ClusterByStatisticsCollectorImpl.create(CLUSTER_BY_XYZ_BUCKET_BY_X, SIGNATURE, MAX_BYTES, MAX_BUCKETS, aggregate, false);
  }
}
