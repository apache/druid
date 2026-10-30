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

package org.apache.druid.query.aggregation.tdigestsketch;

import com.tdunning.math.stats.MergingDigest;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class TDigestSketchAggregatorFactoryTest
{
  @Test
  public void testResultArraySignature()
  {
    final TimeseriesQuery query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2000/3000")
              .granularity(Granularities.HOUR)
              .aggregators(
                  new CountAggregatorFactory("count"),
                  new TDigestSketchAggregatorFactory("tdigest", "col", null, TDigestConfig.builder().maxCompression(200).build())
              )
              .postAggregators(
                  new FieldAccessPostAggregator("tdigest-access", "tdigest"),
                  new FinalizingFieldAccessPostAggregator("tdigest-finalize", "tdigest")
              )
              .build();

    Assertions.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("count", ColumnType.LONG)
                    .add("tdigest", TDigestSketchAggregatorFactory.TYPE)
                    .add("tdigest-access", TDigestSketchAggregatorFactory.TYPE)
                    .add("tdigest-finalize", TDigestSketchAggregatorFactory.TYPE)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }

  @Test
  public void testWithName()
  {
    TDigestSketchAggregatorFactory factory = new TDigestSketchAggregatorFactory("tdigest", "col", null, TDigestConfig.builder().maxCompression(200).build());
    Assertions.assertEquals(factory, factory.withName("tdigest"));
    Assertions.assertEquals("newTest", factory.withName("newTest").getName());
  }

  @Test
  public void testCompressionCappedAtMaxCompression()
  {
    TDigestConfig config = TDigestConfig.builder().maxCompression(150).build();
    TDigestSketchAggregatorFactory factory = new TDigestSketchAggregatorFactory("tdigest", "col", 300, config);
    Assertions.assertEquals(150, factory.getCompression());
  }

  @Test
  public void testCompressionBelowMaxCompressionUnchanged()
  {
    TDigestConfig config = TDigestConfig.builder().maxCompression(150).build();
    TDigestSketchAggregatorFactory factory = new TDigestSketchAggregatorFactory("tdigest", "col", 100, config);
    Assertions.assertEquals(100, factory.getCompression());
  }

  /**
   * Guards {@link TDigestSketchUtils#getMaxIntermdiateTDigestSize} against changes in the t-digest library.
   * The estimate is used for on-heap memory accounting during ingestion, so if it is smaller than what a
   * {@link MergingDigest} really allocates, ingestion persists later than maxBytesInMemory intends.
   */
  @Test
  public void testMaxIntermediateSizeCoversDigestBuffers() throws IllegalAccessException
  {
    // Raise the cap so the factory doesn't reduce the larger compressions below
    final TDigestConfig config = TDigestConfig.builder().maxCompression(1000).build();
    // 5 is raised to the library minimum of 10; 29 and 30 sit on either side of the extra centroid room below 30
    for (int compression : new int[]{5, 10, 29, 30, 100, 200, 1000}) {
      final MergingDigest digest = new MergingDigest(compression);
      // Sum the sizes of the digest's instance arrays (centroids and temp buffers), which dominate its footprint
      long arrayBytes = 0;
      for (Field field : MergingDigest.class.getDeclaredFields()) {
        if (field.getType().isArray() && !Modifier.isStatic(field.getModifiers())) {
          field.setAccessible(true);
          final Object array = field.get(digest);
          if (array != null) {
            final int elementBytes = field.getType().getComponentType() == double.class ? Double.BYTES : Integer.BYTES;
            arrayBytes += (long) Array.getLength(array) * elementBytes;
          }
        }
      }
      final TDigestSketchAggregatorFactory factory = new TDigestSketchAggregatorFactory("sketch", "col", compression, config);
      Assertions.assertTrue(
          factory.getMaxIntermediateSize() >= arrayBytes,
          "compression " + compression + ": estimate " + factory.getMaxIntermediateSize() + " < allocated " + arrayBytes
      );
    }
  }
}
