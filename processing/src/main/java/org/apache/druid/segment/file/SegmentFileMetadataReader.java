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

package org.apache.druid.segment.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.data.CompressionStrategy;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Shared utility for reading the V10 segment file header and metadata. Both {@link SegmentFileMapperV10} (eager,
 * from a local file) and {@link PartialSegmentFileMapperV10} (lazy, from a range-read stream) need to parse the same
 * header format.
 * <p>
 * V10 file format header:
 * {@code | version (byte) | meta compression (byte) | meta length (int) | [compressed length (int)] | meta json |}
 * <p>
 * When metadata is uncompressed, {@code meta length} is the size of the JSON bytes that follow directly.
 * When compressed, an additional 4-byte {@code compressed length} precedes the compressed bytes, and
 * {@code meta length} is the uncompressed size.
 */
public class SegmentFileMetadataReader
{
  /**
   * Size of the fixed portion of the V10 header: version (1) + compression (1) + meta length (4)
   */
  public static final int HEADER_SIZE = 1 + 1 + Integer.BYTES;

  /**
   * Result of reading the V10 header and metadata from a stream.
   */
  public static class Result
  {
    private final SegmentFileMetadata metadata;
    private final long headerSize;

    public Result(SegmentFileMetadata metadata, long headerSize)
    {
      this.metadata = metadata;
      this.headerSize = headerSize;
    }

    /**
     * The parsed segment file metadata.
     */
    public SegmentFileMetadata getMetadata()
    {
      return metadata;
    }

    /**
     * The total size of the header in bytes (everything before the first container). This is needed to compute
     * absolute byte offsets of internal files within the segment file:
     * {@code headerSize + container.startOffset + file.startOffset}
     */
    public long getHeaderSize()
    {
      return headerSize;
    }
  }

  /**
   * Read the V10 metadata from an {@link InputStream}. The stream must be positioned at the start of the V10 file
   * (i.e. at the version byte). After this method returns, the stream will have been read through the end of the
   * metadata section.
   *
   * @param in     input stream positioned at the start of the V10 file
   * @param mapper object mapper for deserializing {@link SegmentFileMetadata}
   * @return the parsed metadata and the total header size in bytes
   */
  public static Result read(InputStream in, ObjectMapper mapper) throws IOException
  {
    final byte[] header = new byte[HEADER_SIZE];
    int read = readFully(in, header);
    if (read < header.length) {
      throw DruidException.defensive("expected at least [%s] bytes, but only read [%s]", header.length, read);
    }
    final ByteBuffer headerBuffer = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);

    if (headerBuffer.get(0) != IndexIO.V10_VERSION) {
      throw DruidException.defensive("not v10, got[%s] instead", headerBuffer.get(0));
    }

    final byte compression = headerBuffer.get(1);
    final CompressionStrategy compressionStrategy = CompressionStrategy.forId(compression);

    final int metaLength = headerBuffer.getInt(2);
    final byte[] meta = new byte[metaLength];

    final long headerSize;
    if (CompressionStrategy.NONE == compressionStrategy) {
      headerSize = HEADER_SIZE + metaLength;
      read = readFully(in, meta);
      if (read < meta.length) {
        throw DruidException.defensive("read[%s] which is less than expected metadata length[%s]", read, metaLength);
      }
    } else {
      final byte[] compressedLengthBytes = new byte[Integer.BYTES];
      read = readFully(in, compressedLengthBytes);
      if (read != Integer.BYTES) {
        throw DruidException.defensive("read[%s] which is less than expected [%s]", read, Integer.BYTES);
      }
      final ByteBuffer compressedLengthBuffer = ByteBuffer.wrap(compressedLengthBytes).order(ByteOrder.LITTLE_ENDIAN);
      final int compressedLength = compressedLengthBuffer.getInt(0);
      headerSize = HEADER_SIZE + Integer.BYTES + compressedLength;

      final byte[] compressed = new byte[compressedLength];
      read = readFully(in, compressed);
      if (read < compressed.length) {
        throw DruidException.defensive(
            "read[%s] which is less than expected compressed metadata length[%s]",
            read,
            compressedLength
        );
      }

      final ByteBuffer inBuffer = ByteBuffer.wrap(compressed).order(ByteOrder.LITTLE_ENDIAN);
      final ByteBuffer outBuffer = ByteBuffer.wrap(meta).order(ByteOrder.LITTLE_ENDIAN);
      final CompressionStrategy.Decompressor decompressor = compressionStrategy.getDecompressor();
      decompressor.decompress(inBuffer, compressedLength, outBuffer);
    }

    final SegmentFileMetadata metadata = mapper.readValue(meta, SegmentFileMetadata.class);
    return new Result(metadata, headerSize);
  }

  /**
   * Read bytes from the stream, retrying until the buffer is full or the stream is exhausted.
   * {@link InputStream#read(byte[])} is not guaranteed to fill the buffer in a single call.
   */
  private static int readFully(InputStream in, byte[] buf) throws IOException
  {
    int offset = 0;
    while (offset < buf.length) {
      int read = in.read(buf, offset, buf.length - offset);
      if (read < 0) {
        break;
      }
      offset += read;
    }
    return offset;
  }

  private SegmentFileMetadataReader()
  {
    // utility class
  }
}
