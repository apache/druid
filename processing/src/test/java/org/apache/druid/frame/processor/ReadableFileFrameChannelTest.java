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

package org.apache.druid.frame.processor;

import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.channel.ReadableFileFrameChannel;
import org.apache.druid.frame.file.FrameFile;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ReadableFileFrameChannelTest extends InitializedNullHandlingTest
{
  private static final int ROWS_PER_FRAME = 20;

  private List<List<Object>> allRows;
  private FrameReader frameReader;
  private FrameFile frameFile;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException
  {
    final CursorFactory cursorFactory = new QueryableIndexCursorFactory(TestIndex.getNoRollupMMappedTestIndex());
    final File file = FrameTestUtil.writeFrameFile(
        FrameSequenceBuilder.fromCursorFactory(cursorFactory)
                            .frameType(FrameType.ROW_BASED)
                            .maxRowsPerFrame(ROWS_PER_FRAME)
                            .frames(),
        temporaryFolder.newFile()
    );
    allRows = FrameTestUtil.readRowsFromCursorFactory(cursorFactory).toList();
    frameReader = FrameReader.create(cursorFactory.getRowSignature());
    frameFile = FrameFile.open(file, null, FrameFile.Flag.DELETE_ON_CLOSE);
  }

  @After
  public void tearDown() throws Exception
  {
    frameFile.close();
  }

  @Test
  public void test_fullFile()
  {
    final ReadableFileFrameChannel channel = new ReadableFileFrameChannel(frameFile);
    Assert.assertTrue(channel.isEntireFile());

    FrameTestUtil.assertRowsEqual(
        Sequences.simple(allRows),
        FrameTestUtil.readRowsFromFrameChannel(channel, frameReader)
    );

    Assert.assertFalse(channel.isEntireFile());
  }

  @Test
  public void test_partialFile()
  {
    final ReadableFileFrameChannel channel = new ReadableFileFrameChannel(frameFile, 1, 2);
    Assert.assertFalse(channel.isEntireFile());

    FrameTestUtil.assertRowsEqual(
        Sequences.simple(allRows).skip(ROWS_PER_FRAME).limit(ROWS_PER_FRAME),
        FrameTestUtil.readRowsFromFrameChannel(channel, frameReader)
    );

    Assert.assertFalse(channel.isEntireFile());
  }
}
