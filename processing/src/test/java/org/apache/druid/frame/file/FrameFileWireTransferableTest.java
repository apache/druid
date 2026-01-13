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

package org.apache.druid.frame.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.channel.ByteTracker;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.wire.FrameWireTransferable;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.rowsandcols.semantic.WireTransferable;
import org.apache.druid.query.rowsandcols.serde.WireTransferableContext;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndexCursorFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

/**
 * Tests error cases for FrameFile with WireTransferable format.
 * Basic read/write functionality is covered by {@link FrameFileTest} with useLegacyFrameSerialization parameter.
 */
public class FrameFileWireTransferableTest extends InitializedNullHandlingTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testRacMethodThrowsWithoutDeserializerForRacEntry() throws IOException
  {
    // Test that rac(int, null) throws when encountering a RAC entry
    final CursorFactory cursorFactory = new IncrementalIndexCursorFactory(
        TestIndex.getNoRollupIncrementalTestIndex()
    );

    // Create wireTransferableContext for RAC serialization
    final ObjectMapper smileMapper = new ObjectMapper();
    final WireTransferable.ConcreteDeserializer deserializer = new WireTransferable.ConcreteDeserializer(
        smileMapper,
        Map.of(
            ByteBuffer.wrap(StringUtils.toUtf8(FrameWireTransferable.TYPE)),
            new FrameWireTransferable.Deserializer()
        )
    );
    final WireTransferableContext wireTransferableContext = new WireTransferableContext(
        smileMapper,
        deserializer,
        false // useLegacyFrameSerialization = false to enable RAC serialization
    );

    // Build frames
    final Sequence<Frame> frames = FrameSequenceBuilder.fromCursorFactory(cursorFactory)
                                                        .frameType(FrameType.latestColumnar())
                                                        .maxRowsPerFrame(50)
                                                        .frames();
    final List<Frame> frameList = frames.toList();

    // Write frame file with useWireTransferableForFrames = true (RAC format)
    final File file = temporaryFolder.newFile();
    try (final FrameFileWriter writer = FrameFileWriter.open(
        Channels.newChannel(Files.newOutputStream(file.toPath())),
        null,
        ByteTracker.unboundedTracker(),
        wireTransferableContext
    )) {
      for (Frame frame : frameList) {
        writer.write(frame.asRAC(), FrameFileWriter.NO_PARTITION);
      }
    }

    // Try to read using rac(int, null) without deserializer - should throw
    try (final FrameFile frameFile = FrameFile.open(file, null)) {
      Assert.assertEquals(frameList.size(), frameFile.numFrames());

      // This should throw DruidException because no deserializer was provided
      Assert.assertThrows(
          "Should throw DruidException when reading RAC entry without deserializer",
          DruidException.class,
          () -> frameFile.rac(0, null)
      );
    }
  }
}
