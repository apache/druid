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

package org.apache.druid.server;

import org.apache.druid.query.context.ResponseContext;

public class ResponseContextConfig
{
  /**
   * The maximum length of {@link ResponseContext} serialized string that might be put into an HTTP response header
   */
  public static final int DEFAULT_RESPONSE_CTX_HEADER_LEN_LIMIT = 7 * 1024;

  private final boolean shouldFailOnTruncatedResponseContext;
  private final int maxResponseContextHeaderSize;

  public static ResponseContextConfig newConfig(boolean shouldFailOnTruncatedResponseContext)
  {
    return new ResponseContextConfig(shouldFailOnTruncatedResponseContext, DEFAULT_RESPONSE_CTX_HEADER_LEN_LIMIT);
  }

  public static ResponseContextConfig forTest(
      boolean shouldFailOnTruncatedResponseContext,
      int maxResponseContextHeaderSize
  )
  {
    return new ResponseContextConfig(shouldFailOnTruncatedResponseContext, maxResponseContextHeaderSize);
  }

  private ResponseContextConfig(boolean shouldFailOnTruncatedResponseContext, int maxResponseContextHeaderSize)
  {
    this.shouldFailOnTruncatedResponseContext = shouldFailOnTruncatedResponseContext;
    this.maxResponseContextHeaderSize = maxResponseContextHeaderSize;
  }

  public boolean shouldFailOnTruncatedResponseContext()
  {
    return shouldFailOnTruncatedResponseContext;
  }

  public int getMaxResponseContextHeaderSize()
  {
    return maxResponseContextHeaderSize;
  }
}
