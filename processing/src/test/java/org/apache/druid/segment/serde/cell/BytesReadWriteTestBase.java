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

import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.segment.data.CompressionStrategy;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

// base class used only for extension
public abstract class BytesReadWriteTestBase implements BytesReadWriteTest
{
  protected final BytesWriterBuilder bytesWriterBuilder;

  private final TestCasesConfig<BytesReadWriteTest> testCases;
  private final ByteWriterTestHelper.ValidationFunctionBuilder validationFunctionBuilder;

  private ByteWriterTestHelper testHelper;

  protected BytesReadWriteTestBase(
      BytesWriterBuilder bytesWriterBuilder,
      ByteWriterTestHelper.ValidationFunctionBuilder validationFunctionBuilder,
      TestCasesConfig<BytesReadWriteTest> testCases
  )
  {
    this.testCases = testCases;
    this.bytesWriterBuilder = bytesWriterBuilder;
    this.validationFunctionBuilder = validationFunctionBuilder;
  }

  protected ByteWriterTestHelper getTestHelper()
  {
    return testHelper;
  }

  @Before
  public void setup()
  {
    testHelper = new ByteWriterTestHelper(bytesWriterBuilder, validationFunctionBuilder);
  }

  @Test
  @Override
  public void testSingleWriteBytes() throws Exception
  {
    Assume.assumeTrue(testCases.isCurrentTestEnabled());

    ByteBuffer payload = testHelper.generateBufferWithLongs(1024);

    runTestWithExceptionHandling(Collections.singletonList(payload), testCases.currentTestValue());
  }

  @Test
  @Override
  public void testSingleMultiBlockWriteBytes() throws Exception
  {
    Assume.assumeTrue(testCases.isCurrentTestEnabled());

    ByteBuffer payload = testHelper.generateBufferWithLongs(256 * 1024); // 2mb

    runTestWithExceptionHandling(Collections.singletonList(payload), testCases.currentTestValue());
  }

  @Test
  @Override
  public void testSingleMultiBlockWriteBytesWithPrelude() throws Exception
  {
    Assume.assumeTrue(testCases.isCurrentTestEnabled());
    ByteBuffer payload1 = testHelper.generateBufferWithLongs(1024); // 8 kb
    ByteBuffer payload2 = testHelper.generateBufferWithLongs(256 * 1024); // 256kb * 8 = 2mb

    runTestWithExceptionHandling(Arrays.asList(payload1, payload2), testCases.currentTestValue());
  }

  @Test
  @Override
  public void testEmptyByteArray() throws Exception
  {
    Assume.assumeTrue(testCases.isCurrentTestEnabled());
    // no-op
    ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
    // block index size: "8" : 4 bytes
    // block index entry 0: "0": 4 bytes
    // block index entry 1: "1": 4 bytes
    // data stream size : "0" : 4 bytes
    runTestWithExceptionHandling(Collections.singletonList(payload), testCases.currentTestValue());
  }

  @Test
  @Override
  public void testNull() throws Exception
  {
    Assume.assumeTrue(testCases.isCurrentTestEnabled());
    TestCaseResult testCaseResult = testCases.currentTestValue();

    runTestWithExceptionHandling(Collections.singletonList(null), testCaseResult);
  }

  @Test
  @Override
  public void testSingleLong() throws Exception
  {
    Assume.assumeTrue(testCases.isCurrentTestEnabled());
    ByteBuffer payload = testHelper.generateBufferWithLongs(1);
    // block index size: "8" : 4 bytes
    // block index entry 0: "0": 4 bytes
    // block index entry 1: "0": 4 bytes
    // data stream size : "1" : 4 bytes
    // compressed single 8 bytes: 9 bytes (compressed: "0")
    runTestWithExceptionHandling(Collections.singletonList(payload), testCases.currentTestValue());
  }

  @Test
  @Override
  public void testVariableSizedCompressablePayloads() throws Exception
  {
    Assume.assumeTrue(testCases.isCurrentTestEnabled());

    List<ByteBuffer> bufferList = testHelper.generateRaggedPayloadBuffer(100, 1024, 10, 0, 0, 10);

    runTestWithExceptionHandling(bufferList, testCases.currentTestValue());
  }

  @Test
  @Override
  public void testOutliersInNormalDataUncompressablePayloads() throws Exception
  {
    Assume.assumeTrue(testCases.isCurrentTestEnabled());

    // every integer within a payload is unique
    List<ByteBuffer> bufferList = testHelper.generateRaggedPayloadBuffer(100, 1024, 10, 64 * 1024, 2);

    runTestWithExceptionHandling(bufferList, testCases.currentTestValue());
  }

  @Test
  @Override
  public void testOutliersInNormalDataCompressablePayloads() throws Exception
  {
    Assume.assumeTrue(testCases.isCurrentTestEnabled());

    // same # of payloads and size of payloads as testOutliersInNormalDataUncompressablePayloads()
    // integer values range 0-9
    List<ByteBuffer> bufferList = testHelper.generateRaggedPayloadBuffer(100, 1024, 10, 64 * 1024, 2, 10);

    runTestWithExceptionHandling(bufferList, testCases.currentTestValue());
  }

  @Test
  @Override
  public void testSingleUncompressableBlock() throws Exception
  {
    Assume.assumeTrue(testCases.isCurrentTestEnabled());

    // every integer within a payload is unique
    ByteBuffer byteBuffer = testHelper.generateIntPayloads(16 * 1024);

    Assert.assertEquals(64 * 1024, byteBuffer.limit());
    // uncompressable 64k block size
    runTestWithExceptionHandling(Collections.singletonList(byteBuffer), testCases.currentTestValue());

  }

  @Test
  @Override
  public void testSingleWriteByteBufferZSTD() throws Exception
  {
    Assume.assumeTrue(testCases.isCurrentTestEnabled());

    ByteBuffer sourcePayLoad = testHelper.generateBufferWithLongs(1024); // 8k

    testHelper.setCompressionStrategy(CompressionStrategy.ZSTD);
    runTestWithExceptionHandling(Collections.singletonList(sourcePayLoad), testCases.currentTestValue());
  }


  @Test
  @Override
  public void testSingleWriteByteBufferAlternateByteBufferProvider() throws Exception
  {
    Assume.assumeTrue(testCases.isCurrentTestEnabled());

    List<ByteBuffer> bufferList = testHelper.generateRaggedPayloadBuffer(100, 1024, 10, 0, 0, 10);

    testHelper.setByteBufferProvider(() -> new ResourceHolder<ByteBuffer>()
    {
      @Override
      public ByteBuffer get()
      {
        return ByteBuffer.allocate(128 * 1024);
      }

      @Override
      public void close()
      {
      }
    });
    runTestWithExceptionHandling(bufferList, testCases.currentTestValue());
  }

  @Test
  @Override
  public void testRandomBlockAccess() throws Exception
  {
    Assume.assumeTrue(testCases.isCurrentTestEnabled());
    //verified that blocks are accessed in random order and the same block is even returned to
    List<ByteBuffer> bufferList = testHelper.generateRaggedPayloadBuffer(8192, 32 * 1024, 256, 256 * 1024, 3, 1024);

    testHelper.setUseRandomReadOrder(true);
    runTestWithExceptionHandling(bufferList, testCases.currentTestValue());
  }

  private void runTestWithExceptionHandling(List<ByteBuffer> bufferList, TestCaseResult testCaseResult) throws Exception
  {
    try {
      testHelper.validateReadAndSize(bufferList, testCaseResult.size);

      if (testCaseResult.exception != null) {
        Assert.fail("expected exception " + testCaseResult.exception.getClass().getName());
      }
    }
    catch (Exception e) {
      if (testCaseResult.exception != null) {
        Assert.assertTrue(testCaseResult.exception.getClass().isAssignableFrom(e.getClass()));
      } else {
        throw e;
      }
    }
  }
}
