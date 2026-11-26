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

package org.apache.druid.java.util.http.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableList;
import io.netty.buffer.AdaptiveByteBufAllocator;
import io.netty.buffer.ByteBufAllocator;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.http.client.pool.ResourcePool;
import org.apache.druid.utils.JvmUtils;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

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
    ZSTD {
      @Override
      public String getEncodingString()
      {
        return "zstd";
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

    /**
     * Parses an encoding name such as "gzip", ignoring case and surrounding whitespace.
     *
     * @throws org.apache.druid.error.DruidException if the name is not a known encoding
     */
    @JsonCreator
    public static CompressionCodec fromString(@Nullable final String name)
    {
      final String trimmed = name == null ? null : name.trim();
      for (final CompressionCodec codec : values()) {
        if (codec.getEncodingString().equalsIgnoreCase(trimmed)) {
          return codec;
        }
      }
      throw InvalidInput.exception(
          "Invalid compression codec[%s]. Supported codecs are: %s.",
          name,
          Arrays.stream(values()).map(CompressionCodec::getEncodingString).toList()
      );
    }
  }

  /**
   * Response encodings the client accepts when {@link Builder#withCompressionCodecs} is not called, most preferred
   * first. Servers that do not support zstd, or have it disabled, respond with gzip.
   */
  public static final List<CompressionCodec> DEFAULT_COMPRESSION_CODECS =
      List.of(CompressionCodec.ZSTD, CompressionCodec.GZIP);

  // Default from SelectorUtil.DEFAULT_IO_THREADS, which is private:
  private static final int DEFAULT_WORKER_COUNT = JvmUtils.getRuntimeInfo().getAvailableProcessors() * 2;

  private static final Duration DEFAULT_UNUSED_CONNECTION_TIMEOUT_DURATION = new Period("PT4M").toStandardDuration();

  // Netty 4 defaults CONNECT_TIMEOUT_MILLIS to 30s; Netty 3 defaulted to 10s. Preserve the older, more
  // aggressive default so a slow or dead peer surfaces sooner. Callers can override via the builder.
  private static final Duration DEFAULT_CONNECT_TIMEOUT_DURATION = new Period("PT10S").toStandardDuration();

  // Set an explicit allocator so callers know exactly what they're getting rather than depending on whatever
  // ByteBufAllocator.DEFAULT happens to resolve to.
  public static final ByteBufAllocator DEFAULT_BYTE_BUF_ALLOCATOR = new AdaptiveByteBufAllocator();

  public static Builder builder()
  {
    return new Builder();
  }

  private final int numConnections;
  private final boolean eagerInitialization;
  private final ResourcePool.Implementation poolImplementation;
  private final SSLContext sslContext;
  private final HttpClientProxyConfig proxyConfig;
  private final Duration readTimeout;
  private final Duration sslHandshakeTimeout;
  private final Duration connectTimeout;
  private final int workerPoolSize;
  @Nullable
  private final List<CompressionCodec> compressionCodecs;
  private final Duration unusedConnectionTimeoutDuration;
  private final ByteBufAllocator byteBufAllocator;

  private HttpClientConfig(
      int numConnections,
      boolean eagerInitialization,
      ResourcePool.Implementation poolImplementation,
      SSLContext sslContext,
      HttpClientProxyConfig proxyConfig,
      Duration readTimeout,
      Duration sslHandshakeTimeout,
      Duration connectTimeout,
      int workerPoolSize,
      @Nullable List<CompressionCodec> compressionCodecs,
      Duration unusedConnectionTimeoutDuration,
      ByteBufAllocator byteBufAllocator
  )
  {
    this.numConnections = numConnections;
    this.eagerInitialization = eagerInitialization;
    this.poolImplementation = poolImplementation;
    this.sslContext = sslContext;
    this.proxyConfig = proxyConfig;
    this.readTimeout = readTimeout;
    this.sslHandshakeTimeout = sslHandshakeTimeout;
    this.connectTimeout = connectTimeout;
    this.workerPoolSize = workerPoolSize;
    this.compressionCodecs = compressionCodecs;
    this.unusedConnectionTimeoutDuration = unusedConnectionTimeoutDuration;
    this.byteBufAllocator = byteBufAllocator;
  }

  public int getNumConnections()
  {
    return numConnections;
  }

  public boolean isEagerInitialization()
  {
    return eagerInitialization;
  }

  public ResourcePool.Implementation getPoolImplementation()
  {
    return poolImplementation;
  }

  public SSLContext getSslContext()
  {
    return sslContext;
  }

  public HttpClientProxyConfig getProxyConfig()
  {
    return proxyConfig;
  }

  public Duration getReadTimeout()
  {
    return readTimeout;
  }

  public Duration getSslHandshakeTimeout()
  {
    return sslHandshakeTimeout;
  }

  public Duration getConnectTimeout()
  {
    return connectTimeout;
  }

  public int getWorkerPoolSize()
  {
    return workerPoolSize;
  }

  /**
   * Response encodings the client accepts, most preferred first, or null if not set, in which case the client accepts
   * {@link #DEFAULT_COMPRESSION_CODECS}.
   */
  @Nullable
  public List<CompressionCodec> getCompressionCodecs()
  {
    return compressionCodecs;
  }

  public Duration getUnusedConnectionTimeoutDuration()
  {
    return unusedConnectionTimeoutDuration;
  }

  public ByteBufAllocator getByteBufAllocator()
  {
    return byteBufAllocator;
  }

  public static class Builder
  {
    private int numConnections = 1;
    private boolean eagerInitialization = true;
    private ResourcePool.Implementation poolImplementation = ResourcePool.Implementation.RETAINING;
    private SSLContext sslContext = null;
    private HttpClientProxyConfig proxyConfig = null;
    private Duration readTimeout = null;
    private Duration sslHandshakeTimeout = null;
    private Duration connectTimeout = DEFAULT_CONNECT_TIMEOUT_DURATION;
    private int workerCount = DEFAULT_WORKER_COUNT;
    @Nullable
    private List<CompressionCodec> compressionCodecs = null;
    private Duration unusedConnectionTimeoutDuration = DEFAULT_UNUSED_CONNECTION_TIMEOUT_DURATION;
    private ByteBufAllocator byteBufAllocator = DEFAULT_BYTE_BUF_ALLOCATOR;

    private Builder()
    {
    }

    public Builder withNumConnections(int numConnections)
    {
      this.numConnections = numConnections;
      return this;
    }

    public Builder withEagerInitialization(boolean eagerInitialization)
    {
      this.eagerInitialization = eagerInitialization;
      return this;
    }

    public Builder withPoolImplementation(ResourcePool.Implementation poolImplementation)
    {
      this.poolImplementation = poolImplementation;
      return this;
    }

    public Builder withSslContext(SSLContext sslContext)
    {
      this.sslContext = sslContext;
      return this;
    }

    public Builder withHttpProxyConfig(HttpClientProxyConfig config)
    {
      this.proxyConfig = config;
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

    /**
     * TCP connect timeout applied to every outbound channel. Null uses the default (10s, matching
     * Netty 3). Configurable so operators can tighten it on latency-sensitive paths or loosen it on
     * links that legitimately take longer to connect.
     */
    public Builder withConnectTimeout(Duration connectTimeout)
    {
      this.connectTimeout = connectTimeout == null ? DEFAULT_CONNECT_TIMEOUT_DURATION : connectTimeout;
      return this;
    }

    public Builder withWorkerCount(int workerCount)
    {
      this.workerCount = workerCount;
      return this;
    }

    /**
     * Sets the only response encoding the client accepts. Null uses {@link #DEFAULT_COMPRESSION_CODECS}.
     */
    public Builder withCompressionCodec(@Nullable CompressionCodec compressionCodec)
    {
      this.compressionCodecs = compressionCodec == null ? null : ImmutableList.of(compressionCodec);
      return this;
    }

    /**
     * Sets the response encodings the client accepts, most preferred first.
     * Duplicates are dropped, keeping the first occurrence.
     * An empty list makes the client ask for uncompressed responses.
     * Null uses {@link #DEFAULT_COMPRESSION_CODECS}.
     */
    public Builder withCompressionCodecs(@Nullable List<CompressionCodec> compressionCodecs)
    {
      this.compressionCodecs = compressionCodecs == null
                               ? null
                               : ImmutableList.copyOf(new LinkedHashSet<>(compressionCodecs));
      return this;
    }

    public Builder withUnusedConnectionTimeoutDuration(Duration unusedConnectionTimeoutDuration)
    {
      this.unusedConnectionTimeoutDuration = unusedConnectionTimeoutDuration;
      return this;
    }

    /**
     * Netty {@link ByteBufAllocator} used for all inbound and outbound buffers on channels created by
     * this client. Null uses the default ({@link AdaptiveByteBufAllocator}, matching Netty 4.2's
     * own picked default). Callers can pass {@code PooledByteBufAllocator.DEFAULT} or
     * {@code UnpooledByteBufAllocator.DEFAULT} to opt out.
     */
    public Builder withByteBufAllocator(ByteBufAllocator byteBufAllocator)
    {
      this.byteBufAllocator = byteBufAllocator == null ? DEFAULT_BYTE_BUF_ALLOCATOR : byteBufAllocator;
      return this;
    }

    public HttpClientConfig build()
    {
      return new HttpClientConfig(
          numConnections,
          eagerInitialization,
          poolImplementation,
          sslContext,
          proxyConfig,
          readTimeout,
          sslHandshakeTimeout,
          connectTimeout,
          workerCount,
          compressionCodecs,
          unusedConnectionTimeoutDuration,
          byteBufAllocator
      );
    }
  }
}
