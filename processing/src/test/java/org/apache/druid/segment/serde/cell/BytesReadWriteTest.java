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

public interface BytesReadWriteTest
{
  void testSingleWriteBytes() throws Exception;

  void testSingleMultiBlockWriteBytes() throws Exception;

  void testSingleMultiBlockWriteBytesWithPrelude() throws Exception;

  void testEmptyByteArray() throws Exception;

  void testNull() throws Exception;

  void testSingleLong() throws Exception;

  void testVariableSizedCompressablePayloads() throws Exception;

  void testOutliersInNormalDataUncompressablePayloads() throws Exception;

  void testOutliersInNormalDataCompressablePayloads() throws Exception;

  void testSingleUncompressableBlock() throws Exception;

  void testSingleWriteByteBufferZSTD() throws Exception;

  void testSingleWriteByteBufferAlternateByteBufferProvider() throws Exception;

  void testRandomBlockAccess() throws Exception;
}
