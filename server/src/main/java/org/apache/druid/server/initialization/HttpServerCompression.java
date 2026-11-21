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


package org.apache.druid.server.initialization;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.http.client.HttpClientConfig.CompressionCodec;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.zip.Deflater;

/**
 * HTTP response compression settings, configured under {@code druid.server.http.compression}.
 */
public class HttpServerCompression
{
  public static final HttpServerCompression DEFAULT = new HttpServerCompression(null, null, null);

  private final List<CompressionCodec> codecs;
  private final Zstd zstd;
  private final Gzip gzip;

  @JsonCreator
  public HttpServerCompression(
      @JsonProperty("codecs") @Nullable final List<String> codecs,
      @JsonProperty("zstd") @Nullable final Zstd zstd,
      @JsonProperty("gzip") @Nullable final Gzip gzip
  )
  {
    this.codecs = codecs == null
                  ? List.of(CompressionCodec.ZSTD, CompressionCodec.GZIP)
                  : codecs.stream()
                          .map(name -> {
                            final CompressionCodec codec = CompressionCodec.fromString(name);
                            if (codec == CompressionCodec.IDENTITY) {
                              throw InvalidInput.exception(
                                  "Invalid HTTP server compression codec[%s]."
                                  + " Set codecs to empty list `[]` to disable response compression.",
                                  name
                              );
                            }
                            return codec;
                          })
                          .distinct()
                          .toList();

    this.zstd = zstd == null ? Zstd.DEFAULT : zstd;
    this.gzip = gzip == null ? Gzip.DEFAULT : gzip;
  }

  /**
   * Response encodings the server may use, most preferred first. Empty if response compression is disabled.
   */
  @JsonProperty("codecs")
  public List<String> getCodecNames()
  {
    return codecs.stream().map(CompressionCodec::getEncodingString).toList();
  }

  @JsonIgnore
  public List<CompressionCodec> getCodecs()
  {
    return codecs;
  }

  @JsonProperty
  public Zstd getZstd()
  {
    return zstd;
  }

  @JsonProperty
  public Gzip getGzip()
  {
    return gzip;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final HttpServerCompression that = (HttpServerCompression) o;
    return codecs.equals(that.codecs) && zstd.equals(that.zstd) && gzip.equals(that.gzip);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(codecs, zstd, gzip);
  }

  @Override
  public String toString()
  {
    return "HttpServerCompression{" +
           "codecs=" + getCodecNames() +
           ", zstd=" + zstd +
           ", gzip=" + gzip +
           '}';
  }

  /**
   * zstd settings. If the level is not set, it is {@link #DEFAULT_LEVEL}.
   */
  public record Zstd(@JsonProperty int level)
  {
    public static final int MIN_LEVEL = 1;
    public static final int MAX_LEVEL = 19;
    public static final int DEFAULT_LEVEL = 1;
    public static final Zstd DEFAULT = new Zstd(DEFAULT_LEVEL);

    public Zstd
    {
      if (level < MIN_LEVEL || level > MAX_LEVEL) {
        throw InvalidInput.exception(
            "Invalid zstd compression level[%d]. It must be between %d and %d.",
            level,
            MIN_LEVEL,
            MAX_LEVEL
        );
      }
    }

    @JsonCreator
    public static Zstd fromJson(@JsonProperty("level") @Nullable final Integer level)
    {
      return new Zstd(level == null ? DEFAULT_LEVEL : level);
    }
  }

  /**
   * gzip settings. A null value means it is not set, so that {@link ServerConfig} can fall back to the deprecated
   * {@code druid.server.http.compressionLevel} and {@code druid.server.http.inflateBufferSize} properties. If neither
   * is set, the defaults are {@link #DEFAULT_LEVEL} and {@link #DEFAULT_INFLATE_BUFFER_SIZE}.
   *
   * {@code inflateBufferSize} is the buffer size of the gzip request decoder. Setting it to 0 disables request
   * decompression for all codecs, as {@code druid.server.http.inflateBufferSize} did.
   */
  public record Gzip(@JsonProperty @Nullable Integer level, @JsonProperty @Nullable Integer inflateBufferSize)
  {
    public static final int MIN_LEVEL = 0;
    public static final int MAX_LEVEL = 9;
    public static final int DEFAULT_LEVEL = Deflater.DEFAULT_COMPRESSION;
    public static final int DEFAULT_INFLATE_BUFFER_SIZE = 4096;
    public static final Gzip DEFAULT = new Gzip(null, null);

    @JsonCreator
    public Gzip
    {
      if (level != null && level != DEFAULT_LEVEL && (level < MIN_LEVEL || level > MAX_LEVEL)) {
        throw InvalidInput.exception(
            "Invalid gzip compression level[%d]. It must be between %d and %d, or %d for the default level.",
            level,
            MIN_LEVEL,
            MAX_LEVEL,
            DEFAULT_LEVEL
        );
      }
      if (inflateBufferSize != null && inflateBufferSize < 0) {
        throw InvalidInput.exception(
            "Invalid gzip inflateBufferSize[%d]. It must be 0 or greater, where 0 disables request decompression.",
            inflateBufferSize
        );
      }
    }

    /**
     * The compression level, or {@link #DEFAULT_LEVEL} if it is not set.
     */
    public int levelOrDefault()
    {
      return level == null ? DEFAULT_LEVEL : level;
    }

    /**
     * The buffer size of the gzip request decoder, or {@link #DEFAULT_INFLATE_BUFFER_SIZE} if it is not set. If it is
     * 0, request bodies are not decompressed.
     */
    public int inflateBufferSizeOrDefault()
    {
      return inflateBufferSize == null ? DEFAULT_INFLATE_BUFFER_SIZE : inflateBufferSize;
    }
  }
}
