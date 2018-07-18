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

package io.druid.java.util.http.client;

import org.joda.time.Duration;
import org.joda.time.Period;

import javax.net.ssl.SSLContext;

/**
 */
public class HttpClientConfig
{
  public enum CompressionCodec
  {
    IDENTITY {
      @Override
      public String getEncodingString()
      {
        return "identity";
      }
    },
    GZIP {
      @Override
      public String getEncodingString()
      {
        return "gzip";
      }
    },
    @SuppressWarnings("unused") //TODO test this CompressionCodec (it counts as usage)
    DEFLATE {
      @Override
      public String getEncodingString()
      {
        return "deflate";
      }
    };

    /**
     * Get the header-ified name of this encoding, which should go in "Accept-Encoding" and
     * "Content-Encoding" headers. This is not just the lowercasing of the enum name, since
     * we may one day support x- encodings like LZ4, which would likely be an enum named
     * "LZ4" that has an encoding string like "x-lz4".
     *
     * @return encoding name
     */
    public abstract String getEncodingString();
  }

  public static final CompressionCodec DEFAULT_COMPRESSION_CODEC = CompressionCodec.GZIP;

  // Default from NioClientSocketChannelFactory.DEFAULT_BOSS_COUNT, which is private:
  private static final int DEFAULT_BOSS_COUNT = 1;

  // Default from SelectorUtil.DEFAULT_IO_THREADS, which is private:
  private static final int DEFAULT_WORKER_COUNT = Runtime.getRuntime().availableProcessors() * 2;

  private static final Duration DEFAULT_UNUSED_CONNECTION_TIMEOUT_DURATION = new Period("PT4M").toStandardDuration();

  public static Builder builder()
  {
    return new Builder();
  }

  private final int numConnections;
  private final SSLContext sslContext;
  private final Duration readTimeout;
  private final Duration sslHandshakeTimeout;
  private final int bossPoolSize;
  private final int workerPoolSize;
  private final CompressionCodec compressionCodec;
  private final Duration unusedConnectionTimeoutDuration;

  @Deprecated // Use the builder instead
  public HttpClientConfig(
      int numConnections,
      SSLContext sslContext
  )
  {
    this(
        numConnections,
        sslContext,
        Duration.ZERO,
        null,
        DEFAULT_BOSS_COUNT,
        DEFAULT_WORKER_COUNT,
        DEFAULT_COMPRESSION_CODEC,
        DEFAULT_UNUSED_CONNECTION_TIMEOUT_DURATION
    );
  }

  @Deprecated // Use the builder instead
  public HttpClientConfig(
      int numConnections,
      SSLContext sslContext,
      Duration readTimeout
  )
  {
    this(
        numConnections,
        sslContext,
        readTimeout,
        null,
        DEFAULT_BOSS_COUNT,
        DEFAULT_WORKER_COUNT,
        DEFAULT_COMPRESSION_CODEC,
        DEFAULT_UNUSED_CONNECTION_TIMEOUT_DURATION
    );
  }

  @Deprecated // Use the builder instead
  public HttpClientConfig(
      int numConnections,
      SSLContext sslContext,
      Duration readTimeout,
      Duration sslHandshakeTimeout
  )
  {
    this(
        numConnections,
        sslContext,
        readTimeout,
        sslHandshakeTimeout,
        DEFAULT_BOSS_COUNT,
        DEFAULT_WORKER_COUNT,
        DEFAULT_COMPRESSION_CODEC,
        DEFAULT_UNUSED_CONNECTION_TIMEOUT_DURATION
    );
  }

  private HttpClientConfig(
      int numConnections,
      SSLContext sslContext,
      Duration readTimeout,
      Duration sslHandshakeTimeout,
      int bossPoolSize,
      int workerPoolSize,
      CompressionCodec compressionCodec,
      Duration unusedConnectionTimeoutDuration
  )
  {
    this.numConnections = numConnections;
    this.sslContext = sslContext;
    this.readTimeout = readTimeout;
    this.sslHandshakeTimeout = sslHandshakeTimeout;
    this.bossPoolSize = bossPoolSize;
    this.workerPoolSize = workerPoolSize;
    this.compressionCodec = compressionCodec;
    this.unusedConnectionTimeoutDuration = unusedConnectionTimeoutDuration;
  }

  public int getNumConnections()
  {
    return numConnections;
  }

  public SSLContext getSslContext()
  {
    return sslContext;
  }

  public Duration getReadTimeout()
  {
    return readTimeout;
  }

  public Duration getSslHandshakeTimeout()
  {
    return sslHandshakeTimeout;
  }

  public int getBossPoolSize()
  {
    return bossPoolSize;
  }

  public int getWorkerPoolSize()
  {
    return workerPoolSize;
  }

  public CompressionCodec getCompressionCodec()
  {
    return compressionCodec;
  }

  public Duration getUnusedConnectionTimeoutDuration()
  {
    return unusedConnectionTimeoutDuration;
  }

  public static class Builder
  {
    private int numConnections = 1;
    private SSLContext sslContext = null;
    private Duration readTimeout = null;
    private Duration sslHandshakeTimeout = null;
    private int bossCount = DEFAULT_BOSS_COUNT;
    private int workerCount = DEFAULT_WORKER_COUNT;
    private CompressionCodec compressionCodec = DEFAULT_COMPRESSION_CODEC;
    private Duration unusedConnectionTimeoutDuration = DEFAULT_UNUSED_CONNECTION_TIMEOUT_DURATION;

    private Builder() {}

    public Builder withNumConnections(int numConnections)
    {
      this.numConnections = numConnections;
      return this;
    }

    public Builder withSslContext(SSLContext sslContext)
    {
      this.sslContext = sslContext;
      return this;
    }

    public Builder withReadTimeout(Duration readTimeout)
    {
      this.readTimeout = readTimeout;
      return this;
    }

    public Builder withSslHandshakeTimeout(Duration sslHandshakeTimeout)
    {
      this.sslHandshakeTimeout = sslHandshakeTimeout;
      return this;
    }

    public Builder withWorkerCount(int workerCount)
    {
      this.workerCount = workerCount;
      return this;
    }

    public Builder withCompressionCodec(CompressionCodec compressionCodec)
    {
      this.compressionCodec = compressionCodec;
      return this;
    }

    public Builder withUnusedConnectionTimeoutDuration(Duration unusedConnectionTimeoutDuration)
    {
      this.unusedConnectionTimeoutDuration = unusedConnectionTimeoutDuration;
      return this;
    }

    public HttpClientConfig build()
    {
      return new HttpClientConfig(
          numConnections,
          sslContext,
          readTimeout,
          sslHandshakeTimeout,
          bossCount,
          workerCount,
          compressionCodec,
          unusedConnectionTimeoutDuration
      );
    }
  }
}
