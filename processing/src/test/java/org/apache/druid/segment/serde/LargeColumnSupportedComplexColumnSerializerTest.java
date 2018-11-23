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

package org.apache.druid.segment.serde;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.Smoosh;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMedium;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;

public class LargeColumnSupportedComplexColumnSerializerTest
{

  private final HashFunction fn = Hashing.murmur3_128();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSanity() throws IOException
  {

    HyperUniquesSerdeForTest serde = new HyperUniquesSerdeForTest(Hashing.murmur3_128());
    int[] cases = {1000, 5000, 10000, 20000};
    int[] columnSizes = {
        Integer.MAX_VALUE,
        Integer.MAX_VALUE / 2,
        Integer.MAX_VALUE / 4,
        5000 * Long.BYTES,
        2500 * Long.BYTES
    };

    for (int columnSize : columnSizes) {
      for (int aCase : cases) {
        File tmpFile = temporaryFolder.newFolder();
        HyperLogLogCollector baseCollector = HyperLogLogCollector.makeLatestCollector();
        try (SegmentWriteOutMedium segmentWriteOutMedium = new OffHeapMemorySegmentWriteOutMedium();
             FileSmoosher v9Smoosher = new FileSmoosher(tmpFile)) {

          LargeColumnSupportedComplexColumnSerializer serializer = LargeColumnSupportedComplexColumnSerializer
              .createWithColumnSize(segmentWriteOutMedium, "test", serde.getObjectStrategy(), columnSize);

          serializer.open();
          for (int i = 0; i < aCase; i++) {
            HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
            byte[] hashBytes = fn.hashLong(i).asBytes();
            collector.add(hashBytes);
            baseCollector.fold(collector);
            serializer.serialize(new ObjectColumnSelector()
            {
              @Nullable
              @Override
              public Object getObject()
              {
                return collector;
              }

              @Override
              public Class classOfObject()
              {
                return HyperLogLogCollector.class;
              }

              @Override
              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
              {
                // doesn't matter in tests
              }
            });
          }

          try (final SmooshedWriter channel = v9Smoosher.addWithSmooshedWriter(
              "test",
              serializer.getSerializedSize()
          )) {
            serializer.writeTo(channel, v9Smoosher);
          }
        }

        SmooshedFileMapper mapper = Smoosh.map(tmpFile);
        final ColumnBuilder builder = new ColumnBuilder()
            .setType(ValueType.COMPLEX)
            .setHasMultipleValues(false)
            .setFileMapper(mapper);
        serde.deserializeColumn(mapper.mapFile("test"), builder);

        ColumnHolder columnHolder = builder.build();
        ComplexColumn complexColumn = (ComplexColumn) columnHolder.getColumn();
        HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

        for (int i = 0; i < aCase; i++) {
          collector.fold((HyperLogLogCollector) complexColumn.getRowValue(i));
        }
        Assert.assertEquals(baseCollector.estimateCardinality(), collector.estimateCardinality(), 0.0);
      }
    }
  }

}
