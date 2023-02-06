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
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class SequenceMetadataTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Mock
  private SeekableStreamIndexTaskRunner mockSeekableStreamIndexTaskRunner;

  @Mock
  private SeekableStreamEndSequenceNumbers mockSeekableStreamEndSequenceNumbers;

  @Mock
  private TaskActionClient mockTaskActionClient;

  @Mock
  private TaskToolbox mockTaskToolbox;

  @Test
  public void testPublishAnnotatedSegmentsThrowExceptionIfOverwriteSegmentsNotNullAndNotEmpty() throws Exception
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
        ImmutableSet.of()
    );
    TransactionalSegmentPublisher transactionalSegmentPublisher = sequenceMetadata.createPublisher(mockSeekableStreamIndexTaskRunner, mockTaskToolbox, true);

    expectedException.expect(ISE.class);
    expectedException.expectMessage(
        "Stream ingestion task unexpectedly attempted to overwrite segments: " + SegmentUtils.commaSeparatedIdentifiers(notNullNotEmptySegment)
    );

    transactionalSegmentPublisher.publishAnnotatedSegments(notNullNotEmptySegment, null, ImmutableSet.of(), null);
  }

  @Test
  public void testPublishAnnotatedSegmentsThrowExceptionIfDropSegmentsNotNullAndNotEmpty() throws Exception
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
        ImmutableSet.of()
    );
    TransactionalSegmentPublisher transactionalSegmentPublisher = sequenceMetadata.createPublisher(mockSeekableStreamIndexTaskRunner, mockTaskToolbox, true);

    expectedException.expect(ISE.class);
    expectedException.expectMessage(
        "Stream ingestion task unexpectedly attempted to drop segments: " + SegmentUtils.commaSeparatedIdentifiers(notNullNotEmptySegment)
    );

    transactionalSegmentPublisher.publishAnnotatedSegments(null, notNullNotEmptySegment, ImmutableSet.of(), null);
  }

  @Test
  public void testPublishAnnotatedSegmentsSucceedIfDropSegmentsAndOverwriteSegmentsNullAndEmpty() throws Exception
  {
    Mockito.when(mockSeekableStreamIndexTaskRunner.deserializePartitionsFromMetadata(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(mockSeekableStreamEndSequenceNumbers);
    Mockito.when(mockSeekableStreamEndSequenceNumbers.getPartitionSequenceNumberMap()).thenReturn(ImmutableMap.of());
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
        ImmutableSet.of()
    );
    TransactionalSegmentPublisher transactionalSegmentPublisher = sequenceMetadata.createPublisher(mockSeekableStreamIndexTaskRunner, mockTaskToolbox, false);

    transactionalSegmentPublisher.publishAnnotatedSegments(null, null, notNullNotEmptySegment, ImmutableMap.of());
  }
}
