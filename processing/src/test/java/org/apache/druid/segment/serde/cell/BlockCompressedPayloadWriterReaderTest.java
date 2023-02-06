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

import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;

/**
 * tests both {@link BlockCompressedPayloadWriter} and {@link BlockCompressedPayloadReader}
 */
public class BlockCompressedPayloadWriterReaderTest extends BytesReadWriteTestBase
{
  public BlockCompressedPayloadWriterReaderTest()
  {
    super(
        new BlockCompressedPayloadWriterToBytesWriter.Builder(
            new BlockCompressedPayloadWriter.Builder(
                new OnHeapMemorySegmentWriteOutMedium()
            )
        ),
        ByteWriterTestHelper.ValidationFunctionBuilder.PAYLOAD_WRITER_VALIDATION_FUNCTION_FACTORY,
        new BytesReadWriteTestCases()
            .setTestCaseValue(BytesReadWriteTest::testSingleWriteBytes, TestCaseResult.of(4115))
            .setTestCaseValue(BytesReadWriteTest::testSingleMultiBlockWriteBytes, TestCaseResult.of(1049169))
            .setTestCaseValue(BytesReadWriteTest::testSingleMultiBlockWriteBytesWithPrelude, TestCaseResult.of(1053238))
            // BytesReadWriteTest::testEmptyByteArray -> compression header is 12-bytes
            .setTestCaseValue(BytesReadWriteTest::testEmptyByteArray, TestCaseResult.of(12))
            .setTestCaseValue(BytesReadWriteTest::testNull, TestCaseResult.of(new NullPointerException()))
            .setTestCaseValue(BytesReadWriteTest::testSingleLong, TestCaseResult.of(25))
            .setTestCaseValue(BytesReadWriteTest::testVariableSizedCompressablePayloads, TestCaseResult.of(1180))
            .setTestCaseValue(
                BytesReadWriteTest::testOutliersInNormalDataUncompressablePayloads,
                TestCaseResult.of(574302)
            )
            .setTestCaseValue(BytesReadWriteTest::testOutliersInNormalDataCompressablePayloads, TestCaseResult.of(5997))
            .setTestCaseValue(BytesReadWriteTest::testSingleUncompressableBlock, TestCaseResult.of(65715))
            .setTestCaseValue(BytesReadWriteTest::testSingleWriteByteBufferZSTD, TestCaseResult.of(796))
            .setTestCaseValue(
                BytesReadWriteTest::testSingleWriteByteBufferAlternateByteBufferProvider,
                TestCaseResult.of(1077)
            )
            .setTestCaseValue(BytesReadWriteTest::testRandomBlockAccess, TestCaseResult.of(3124842))
    );
  }
}
