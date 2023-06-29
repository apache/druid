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

public class TestCaseResult
{
  public final byte[] bytes;
  public final int size;
  public final Exception exception;

  private TestCaseResult(byte[] bytes, int size, Exception exception)
  {
    this.bytes = bytes;
    this.size = size;
    this.exception = exception;
  }

  public static TestCaseResult of(Exception exception)
  {
    return new TestCaseResult(null, -1, exception);
  }

  public static TestCaseResult of(int sizeBytes)
  {
    return new TestCaseResult(null, sizeBytes, null);
  }

  public static TestCaseResult of(byte[] bytes)
  {
    return new TestCaseResult(bytes, bytes.length, null);
  }

  public static TestCaseResult of(byte[] bytes, int sizeBytes)
  {
    return new TestCaseResult(bytes, sizeBytes, null);
  }
}
