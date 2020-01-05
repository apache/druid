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

package org.apache.druid.segment.data;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class GenericIndexedWriterTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void staticSetUp()
  {
    NullHandling.initializeForTests();
  }

  @Test
  public void writeLargeValueIntoLargeColumn() throws IOException
  {
    // Regression test for https://github.com/apache/druid/issues/9027.

    final GenericIndexedWriter<String> writer = new GenericIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        "test",
        GenericIndexed.STRING_STRATEGY,
        100
    );

    writer.setIntMaxForCasting(150);
    writer.open();
    writer.write("i really like writing strings");
    writer.write("i really like writing strings");
    writer.write("i really like writing strings i really like writing strings i really like writing strings");
    writer.write("i really like writing strings");
    writer.writeTo(
        FileChannel.open(temporaryFolder.newFile().toPath(), StandardOpenOption.WRITE),
        new FileSmoosher(temporaryFolder.newFolder())
    );
  }
}
