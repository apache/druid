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

package org.apache.druid.segment.serde.cell;

import com.google.common.primitives.Ints;
import org.apache.druid.segment.writeout.HeapByteBufferWriteOutBytes;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * tests both {@link CellWriter} and {@link CellReader}
 */
public class CellWriterReaderTest extends BytesReadWriteTestBase
{
  public CellWriterReaderTest()
  {
    super(
        new CellWriterToBytesWriter.Builder(
            new CellWriter.Builder(new OnHeapMemorySegmentWriteOutMedium())
        ),
        ByteWriterTestHelper.ValidationFunctionBuilder.CELL_READER_VALIDATION_FUNCTION_FACTORY,
        new BytesReadWriteTestCases()
            .setTestCaseValue(BytesReadWriteTest::testSingleLong, TestCaseResult.of(62))
            .setTestCaseValue(BytesReadWriteTest::testEmptyByteArray, TestCaseResult.of(46))
            .setTestCaseValue(BytesReadWriteTest::testNull, TestCaseResult.of(46))
            .setTestCaseValue(BytesReadWriteTest::testSingleWriteBytes, TestCaseResult.of(4151))
            .setTestCaseValue(BytesReadWriteTest::testSingleMultiBlockWriteBytes, TestCaseResult.of(1049204))
            .setTestCaseValue(BytesReadWriteTest::testSingleMultiBlockWriteBytesWithPrelude, TestCaseResult.of(1053277))
            .setTestCaseValue(BytesReadWriteTest::testVariableSizedCompressablePayloads, TestCaseResult.of(1655))
            .setTestCaseValue(BytesReadWriteTest::testOutliersInNormalDataCompressablePayloads, TestCaseResult.of(7368))
            .setTestCaseValue(
                BytesReadWriteTest::testOutliersInNormalDataUncompressablePayloads,
                TestCaseResult.of(575673)
            )
            .setTestCaseValue(BytesReadWriteTest::testSingleUncompressableBlock, TestCaseResult.of(65750))
            .setTestCaseValue(BytesReadWriteTest::testSingleWriteByteBufferZSTD, TestCaseResult.of(845))
            .setTestCaseValue(
                BytesReadWriteTest::testSingleWriteByteBufferAlternateByteBufferProvider,
                TestCaseResult.of(1552)
            )
            .setTestCaseValue(BytesReadWriteTest::testRandomBlockAccess, TestCaseResult.of(3126618))
    );
  }

  @Test
  public void testBasic() throws Exception
  {
    // generates a list of randomly variable-sized payloads in a range
    List<ByteBuffer> byteBufferList = getTestHelper().generateRaggedPayloadBuffer(
        500,
        2048,
        25,
        64 * 1024,
        2,
        10 * 1024
    );
    // for test only, we store bytes heap buffers
    HeapByteBufferWriteOutBytes writableChannel = new HeapByteBufferWriteOutBytes();
    long size;

    try (CellWriter cellWriter = CellWriter.builder(new OnHeapMemorySegmentWriteOutMedium()).build()) {
      // write our payloads
      for (ByteBuffer byteBuffer : byteBufferList) {
        cellWriter.write(byteBuffer);
      }
      // finalize the internal buffer in the CellWriter
      cellWriter.close();
      // transfter the buffer to our WritableByteChannel
      cellWriter.writeTo(writableChannel, null);
      size = cellWriter.getSerializedSize();
    }

    // transfer the bytes into a ByteBuffer. Normally the WritableByteChannel would be to a file that could be
    // memory mapped into a ByteBuffer
    ByteBuffer storedByteBuffer = ByteBuffer.allocate(Ints.checkedCast(size));

    writableChannel.readFully(0, storedByteBuffer);
    storedByteBuffer.flip();

    try (CellReader cellReader = CellReader.builder(storedByteBuffer).build()) {
      for (int i = 0; i < byteBufferList.size(); i++) {
        ByteBuffer buffer = byteBufferList.get(i);
        ByteBuffer readCell = cellReader.getCell(i);

        Assert.assertEquals(buffer, readCell);
      }
    }
  }
}
