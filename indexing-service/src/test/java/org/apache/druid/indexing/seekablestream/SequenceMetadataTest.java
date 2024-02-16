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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class SequenceMetadataTest
{
  @Mock
  private SeekableStreamIndexTaskRunner mockSeekableStreamIndexTaskRunner;

  @Mock
  private SeekableStreamEndSequenceNumbers mockSeekableStreamEndSequenceNumbers;

  @Mock
  private TaskActionClient mockTaskActionClient;

  @Mock
  private TaskToolbox mockTaskToolbox;

  @Test
  public void testPublishAnnotatedSegmentsThrowExceptionIfOverwriteSegmentsNotNullAndNotEmpty()
  {
    DataSegment dataSegment = DataSegment.builder()
                                         .dataSource("foo")
                                         .interval(Intervals.of("2001/P1D"))
                                         .shardSpec(new LinearShardSpec(1))
                                         .version("b")
                                         .size(0)
                                         .build();

    Set<DataSegment> notNullNotEmptySegment = ImmutableSet.of(dataSegment);
    SequenceMetadata<Integer, Integer> sequenceMetadata = new SequenceMetadata<>(
        1,
        "test",
        ImmutableMap.of(),
        ImmutableMap.of(),
        true,
        ImmutableSet.of(),
        null
    );
    TransactionalSegmentPublisher transactionalSegmentPublisher
        = sequenceMetadata.createPublisher(mockSeekableStreamIndexTaskRunner, mockTaskToolbox, true);

    ISE exception = Assert.assertThrows(
        ISE.class,
        () -> transactionalSegmentPublisher.publishAnnotatedSegments(notNullNotEmptySegment, ImmutableSet.of(), null, null)
    );
    Assert.assertEquals(
        "Stream ingestion task unexpectedly attempted to overwrite segments: "
        + SegmentUtils.commaSeparatedIdentifiers(notNullNotEmptySegment),
        exception.getMessage()
    );
  }

  @Test
  public void testPublishAnnotatedSegmentsSucceedIfDropSegmentsAndOverwriteSegmentsNullAndEmpty() throws Exception
  {
    Mockito.when(
               mockSeekableStreamIndexTaskRunner.deserializePartitionsFromMetadata(
                   ArgumentMatchers.any(),
                   ArgumentMatchers.any()
               ))
           .thenReturn(mockSeekableStreamEndSequenceNumbers);
    Mockito.when(mockSeekableStreamEndSequenceNumbers.getPartitionSequenceNumberMap()).thenReturn(ImmutableMap.of());
    Mockito.when(mockSeekableStreamEndSequenceNumbers.getStream()).thenReturn("stream");
    Mockito.when(mockTaskToolbox.getTaskActionClient()).thenReturn(mockTaskActionClient);
    DataSegment dataSegment = DataSegment.builder()
                                         .dataSource("foo")
                                         .interval(Intervals.of("2001/P1D"))
                                         .shardSpec(new LinearShardSpec(1))
                                         .version("b")
                                         .size(0)
                                         .build();

    Set<DataSegment> notNullNotEmptySegment = ImmutableSet.of(dataSegment);
    SequenceMetadata<Integer, Integer> sequenceMetadata = new SequenceMetadata<>(
        1,
        "test",
        ImmutableMap.of(),
        ImmutableMap.of(),
        true,
        ImmutableSet.of(0),
        null
    );
    TransactionalSegmentPublisher transactionalSegmentPublisher = sequenceMetadata.createPublisher(mockSeekableStreamIndexTaskRunner, mockTaskToolbox, false);

    transactionalSegmentPublisher.publishAnnotatedSegments(null, notNullNotEmptySegment, ImmutableMap.of(), null);

    transactionalSegmentPublisher = sequenceMetadata.createPublisher(mockSeekableStreamIndexTaskRunner, mockTaskToolbox, true);

    transactionalSegmentPublisher.publishAnnotatedSegments(null, notNullNotEmptySegment, ImmutableMap.of(), null);
  }

  @Test
  public void testCanHandle()
  {
    SequenceMetadata<Integer, Integer> sequenceMetadata = new SequenceMetadata<>(
        1,
        "test",
        ImmutableMap.of(0, 0),
        ImmutableMap.of(),
        true,
        ImmutableSet.of(0),
        null
    );

    OrderedPartitionableRecord<Integer, Integer, ?> record = new OrderedPartitionableRecord<>(
        "stream",
        0,
        0,
        Collections.singletonList(new ByteEntity(StringUtils.toUtf8("unparseable")))
    );

    Mockito.when(mockSeekableStreamIndexTaskRunner.createSequenceNumber(ArgumentMatchers.any())).thenReturn(makeSequenceNumber("1", false));
    Mockito.when(mockSeekableStreamIndexTaskRunner.isEndOffsetExclusive()).thenReturn(true);
    Assert.assertFalse(sequenceMetadata.canHandle(mockSeekableStreamIndexTaskRunner, record));

    Mockito.when(mockSeekableStreamIndexTaskRunner.isEndOffsetExclusive()).thenReturn(false);
    Assert.assertFalse(sequenceMetadata.canHandle(mockSeekableStreamIndexTaskRunner, record));
  }

  private OrderedSequenceNumber<String> makeSequenceNumber(String seq, boolean isExclusive)
  {
    return new OrderedSequenceNumber<String>(seq, isExclusive)
    {
      @Override
      public int compareTo(OrderedSequenceNumber<String> o)
      {
        return new BigInteger(this.get()).compareTo(new BigInteger(o.get()));
      }

      @Override
      public boolean equals(Object o)
      {
        if (o.getClass() != this.getClass()) {
          return false;
        }
        return new BigInteger(this.get()).equals(new BigInteger(((OrderedSequenceNumber<String>) o).get()));
      }

      @Override
      public int hashCode()
      {
        return super.hashCode();
      }
    };
  }
}
