/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.java.util.emitter.core;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;

public class BaseHttpEmittingConfig
{
  public static final long DEFAULT_FLUSH_MILLIS = 60 * 1000;
  public static final int DEFAULT_FLUSH_COUNTS = 500;
  public static final int DEFAULT_MAX_BATCH_SIZE = 5 * 1024 * 1024;
  /**
   * Do not time out in case flushTimeOut is not set
   */
  public static final long DEFAULT_FLUSH_TIME_OUT = Long.MAX_VALUE;
  public static final String DEFAULT_BASIC_AUTHENTICATION = null;
  public static final BatchingStrategy DEFAULT_BATCHING_STRATEGY = BatchingStrategy.ARRAY;
  public static final ContentEncoding DEFAULT_CONTENT_ENCODING = null;
  public static final int DEFAULT_BATCH_QUEUE_SIZE_LIMIT = 50;
  public static final float DEFAULT_HTTP_TIMEOUT_ALLOWANCE_FACTOR = 2.0f;
  /**
   * The default value effective doesn't set the min timeout
   */
  public static final int DEFAULT_MIN_HTTP_TIMEOUT_MILLIS = 0;

  @Min(1)
  @JsonProperty
  long flushMillis = DEFAULT_FLUSH_MILLIS;

  @Min(0)
  @JsonProperty
  int flushCount = DEFAULT_FLUSH_COUNTS;

  @Min(0)
  @JsonProperty
  long flushTimeOut = DEFAULT_FLUSH_TIME_OUT;

  @JsonProperty
  String basicAuthentication = DEFAULT_BASIC_AUTHENTICATION;

  @JsonProperty
  BatchingStrategy batchingStrategy = DEFAULT_BATCHING_STRATEGY;

  @Min(0)
  @JsonProperty
  int maxBatchSize = DEFAULT_MAX_BATCH_SIZE;

  @JsonProperty
  ContentEncoding contentEncoding = DEFAULT_CONTENT_ENCODING;

  @Min(0)
  @JsonProperty
  int batchQueueSizeLimit = DEFAULT_BATCH_QUEUE_SIZE_LIMIT;

  @Min(1)
  @JsonProperty
  float httpTimeoutAllowanceFactor = DEFAULT_HTTP_TIMEOUT_ALLOWANCE_FACTOR;

  @Min(0)
  @JsonProperty
  int minHttpTimeoutMillis = DEFAULT_MIN_HTTP_TIMEOUT_MILLIS;

  public long getFlushMillis()
  {
    return flushMillis;
  }

  public int getFlushCount()
  {
    return flushCount;
  }

  public long getFlushTimeOut()
  {
    return flushTimeOut;
  }

  public String getBasicAuthentication()
  {
    return basicAuthentication;
  }

  public BatchingStrategy getBatchingStrategy()
  {
    return batchingStrategy;
  }

  public int getMaxBatchSize()
  {
    return maxBatchSize;
  }

  public ContentEncoding getContentEncoding()
  {
    return contentEncoding;
  }

  public int getBatchQueueSizeLimit()
  {
    return batchQueueSizeLimit;
  }

  public float getHttpTimeoutAllowanceFactor()
  {
    return httpTimeoutAllowanceFactor;
  }

  public int getMinHttpTimeoutMillis()
  {
    return minHttpTimeoutMillis;
  }

  @Override
  public String toString()
  {
    return "BaseHttpEmittingConfig{" + toStringBase() + '}';
  }

  protected String toStringBase()
  {
    return
        "flushMillis=" + flushMillis +
        ", flushCount=" + flushCount +
        ", flushTimeOut=" + flushTimeOut +
        ", basicAuthentication='" + basicAuthentication + '\'' +
        ", batchingStrategy=" + batchingStrategy +
        ", maxBatchSize=" + maxBatchSize +
        ", contentEncoding=" + contentEncoding +
        ", batchQueueSizeLimit=" + batchQueueSizeLimit +
        ", httpTimeoutAllowanceFactor=" + httpTimeoutAllowanceFactor +
        ", minHttpTimeoutMillis=" + minHttpTimeoutMillis;
  }
}
