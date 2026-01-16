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
import com.google.common.primitives.Ints;
import org.apache.druid.error.DruidException;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.projections.ProjectionMetadata;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * {@link SegmentFileBuilder} for V10 format segments. Right now, this uses a {@link FileSmoosher} underneath to build
 * V9 smoosh files and collect the metadata about the offsets in those containers, and then appends them into the V10
 * consolidated segment file after the header and {@link SegmentFileMetadata} is written.
 * <p>
 * V10 file format:
 * | version (byte) | meta compression (byte) | meta length (int) | meta json | container 0 | ... | container n |
 */
public class SegmentFileBuilderV10 implements SegmentFileBuilder
{
  public static SegmentFileBuilderV10 create(ObjectMapper jsonMapper, File baseDir)
  {
    return create(jsonMapper, baseDir, CompressionStrategy.NONE);
  }

  public static SegmentFileBuilderV10 create(ObjectMapper jsonMapper, File baseDir, CompressionStrategy metaCompression)
  {
    return new SegmentFileBuilderV10(
        jsonMapper,
        IndexIO.V10_FILE_NAME,
        baseDir,
        Integer.MAX_VALUE,
        metaCompression
    );
  }

  private final ObjectMapper jsonMapper;
  private final String outputFileName;
  private final File baseDir;
  private final long maxChunkSize;
  private final CompressionStrategy metadataCompression;
  private final FileSmoosher smoosher;
  private final Map<String, SegmentFileBuilderV10> externalSegmentFileBuilders;
  private final Map<String, ColumnDescriptor> columns = new TreeMap<>();

  @Nullable
  private String interval = null;
  @Nullable
  private BitmapSerdeFactory bitmapEncoding = null;
  @Nullable
  private List<ProjectionMetadata> projections = null;

  private SegmentFileBuilderV10(
      ObjectMapper jsonMapper,
      String outputFileName,
      File baseDir,
      long maxChunkSize,
      CompressionStrategy metadataCompression
  )
  {
    this.jsonMapper = jsonMapper;
    this.outputFileName = outputFileName;
    this.baseDir = baseDir;
    this.maxChunkSize = maxChunkSize;
    this.metadataCompression = metadataCompression;
    this.smoosher = new FileSmoosher(baseDir, Ints.checkedCast(maxChunkSize), outputFileName);
    this.externalSegmentFileBuilders = new TreeMap<>();
  }

  @Override
  public void add(String name, File fileToAdd) throws IOException
  {
    smoosher.add(name, fileToAdd);
  }

  @Override
  public void add(String name, ByteBuffer bufferToAdd) throws IOException
  {
    smoosher.add(name, bufferToAdd);
  }

  @Override
  public SegmentFileChannel addWithChannel(String name, long size) throws IOException
  {
    return smoosher.addWithChannel(name, size);
  }

  @Override
  public SegmentFileBuilder getExternalBuilder(String externalFile)
  {
    return externalSegmentFileBuilders.computeIfAbsent(
        externalFile,
        (k) -> new SegmentFileBuilderV10(jsonMapper, externalFile, baseDir, maxChunkSize, metadataCompression)
    );
  }

  @Override
  public void addColumn(String name, ColumnDescriptor columnDescriptor)
  {
    this.columns.put(name, columnDescriptor);
  }

  public void addInterval(String interval)
  {
    this.interval = interval;
  }

  public void addBitmapEncoding(BitmapSerdeFactory bitmapEncoding)
  {
    this.bitmapEncoding = bitmapEncoding;
  }

  public void addProjections(List<ProjectionMetadata> projections)
  {
    this.projections = projections;
  }

  @Override
  public void abort()
  {
    smoosher.abort();
  }

  @Override
  public void close() throws IOException
  {
    for (SegmentFileBuilderV10 externalBuilder : externalSegmentFileBuilders.values()) {
      externalBuilder.close();
    }

    smoosher.close();

    SegmentFileMetadata segmentFileMetadata = new SegmentFileMetadata(
        smoosher.getContainers(),
        smoosher.getInternalFiles(),
        interval,
        columns.isEmpty() ? null : columns,
        projections,
        bitmapEncoding
    );

    final byte[] metadataBytes = jsonMapper.writeValueAsBytes(segmentFileMetadata);

    try (final Closer closer = Closer.create()) {
      final FileOutputStream outputStream = closer.register(new FileOutputStream(new File(baseDir, outputFileName)));
      final FileChannel channel = closer.register(outputStream.getChannel());
      final ByteBuffer intBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);

      outputStream.write(new byte[]{IndexIO.V10_VERSION, metadataCompression.getId()});
      // write uncompressed metadata length
      intBuffer.putInt(metadataBytes.length);
      intBuffer.flip();
      outputStream.write(intBuffer.array());

      if (CompressionStrategy.NONE == metadataCompression) {
        // no compression, just write the plain metadata bytes
        outputStream.write(metadataBytes);
      } else {
        // compress the data using the strategy, write the compressed length, then the compressed blob
        final CompressionStrategy.Compressor compressor = metadataCompression.getCompressor();
        final ByteBuffer inBuffer = compressor.allocateInBuffer(metadataBytes.length, closer)
                                              .order(ByteOrder.nativeOrder());
        inBuffer.put(metadataBytes, 0, metadataBytes.length);
        inBuffer.flip();

        final ByteBuffer outBuffer = compressor.allocateOutBuffer(metadataBytes.length, closer)
                                               .order(ByteOrder.nativeOrder());
        final ByteBuffer compressed = compressor.compress(inBuffer, outBuffer);

        // write compression length
        intBuffer.position(0);
        intBuffer.putInt(compressed.remaining());
        intBuffer.flip();
        outputStream.write(intBuffer.array());

        // write compressed metadata
        Channels.writeFully(channel, compressed);
      }

      for (File f : smoosher.getOutFiles()) {
        try (FileInputStream fis = new FileInputStream(f)) {
          byte[] buffer = new byte[4096];
          int bytesRead;
          while ((bytesRead = fis.read(buffer)) != -1) {
            outputStream.write(buffer, 0, bytesRead);
          }
        }
        // delete all the old 00000.smoosh
        DruidException.conditionalDefensive(
            f.delete(),
            "Failed to delete temporary file[%s]",
            f
        );
      }
    }
  }
}
