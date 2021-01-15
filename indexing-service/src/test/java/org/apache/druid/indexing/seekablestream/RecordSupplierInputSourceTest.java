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

import com.google.common.collect.Maps;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RecordSupplierInputSourceTest extends InitializedNullHandlingTest
{

  private static final int NUM_COLS = 16;
  private static final int NUM_ROWS = 128;
  private static final String TIMESTAMP_STRING = "2019-01-01";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testRead() throws IOException
  {
    final RandomCsvSupplier supplier = new RandomCsvSupplier();
    final InputSource inputSource = new RecordSupplierInputSource<>("topic", supplier, false);
    final List<String> colNames = IntStream.range(0, NUM_COLS)
                                           .mapToObj(i -> StringUtils.format("col_%d", i))
                                           .collect(Collectors.toList());
    final InputFormat inputFormat = new CsvInputFormat(colNames, null, null, false, 0);
    final InputSourceReader reader = inputSource.reader(
        new InputRowSchema(
            new TimestampSpec("col_0", "auto", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(colNames.subList(1, colNames.size()))),
            Collections.emptyList()
        ),
        inputFormat,
        temporaryFolder.newFolder()
    );

    int read = 0;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      for (; read < NUM_ROWS && iterator.hasNext(); read++) {
        final InputRow inputRow = iterator.next();
        Assert.assertEquals(DateTimes.of(TIMESTAMP_STRING), inputRow.getTimestamp());
        Assert.assertEquals(NUM_COLS - 1, inputRow.getDimensions().size());
      }
    }

    Assert.assertEquals(NUM_ROWS, read);
    Assert.assertTrue(supplier.isClosed());
  }

  private static class RandomCsvSupplier implements RecordSupplier<Integer, Integer, ByteEntity>
  {
    private static final int STR_LEN = 8;

    private final Random random = ThreadLocalRandom.current();
    private final Map<Integer, Integer> partitionToOffset;

    private volatile boolean closed = false;

    private RandomCsvSupplier()
    {
      partitionToOffset = Maps.newHashMapWithExpectedSize(3);
      for (int i = 0; i < 3; i++) {
        partitionToOffset.put(i, 0);
      }
    }

    @Override
    public void assign(Set<StreamPartition<Integer>> streamPartitions)
    {
      // do nothing
    }

    @Override
    public void seekToEarliest(Set<StreamPartition<Integer>> streamPartitions)
    {
      // do nothing
    }

    @Override
    public void seekToLatest(Set<StreamPartition<Integer>> streamPartitions)
    {
      // do nothing
    }

    @NotNull
    @Override
    public List<OrderedPartitionableRecord<Integer, Integer, ByteEntity>> poll(long timeout)
    {
      final long sleepTime = random.nextInt((int) timeout);
      try {
        Thread.sleep(sleepTime);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      if (sleepTime == timeout) {
        return Collections.emptyList();
      } else {
        final int numRecords = random.nextInt(8); // can be 0
        final List<OrderedPartitionableRecord<Integer, Integer, ByteEntity>> records = new ArrayList<>(numRecords);
        for (int i = 0; i < numRecords; i++) {
          final int partitionId = random.nextInt(partitionToOffset.size());
          final int offset = partitionToOffset.get(partitionId);
          final int numBytes = random.nextInt(3); // can be 0
          final List<ByteEntity> bytes = IntStream
              .range(0, numBytes)
              .mapToObj(j -> {
                final List<String> columns = new ArrayList<>(NUM_COLS);
                columns.add(TIMESTAMP_STRING); // timestamp
                for (int k = 0; k < NUM_COLS - 1; k++) {
                  columns.add(RandomStringUtils.random(STR_LEN, true, false));
                }
                return new ByteEntity(StringUtils.toUtf8(String.join(",", columns)));
              })
              .collect(Collectors.toList());
          records.add(new OrderedPartitionableRecord<>("topic", partitionId, offset, bytes));
        }
        return records;
      }
    }

    @Override
    public Set<Integer> getPartitionIds(String stream)
    {
      return partitionToOffset.keySet();
    }

    @Override
    public void close()
    {
      closed = true;
    }

    boolean isClosed()
    {
      return closed;
    }

    @Override
    public void seek(StreamPartition<Integer> partition, Integer sequenceNumber)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection<StreamPartition<Integer>> getAssignment()
    {
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Integer getLatestSequenceNumber(StreamPartition<Integer> partition)
    {
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Integer getEarliestSequenceNumber(StreamPartition<Integer> partition)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Integer getPosition(StreamPartition<Integer> partition)
    {
      throw new UnsupportedOperationException();
    }
  }
}
