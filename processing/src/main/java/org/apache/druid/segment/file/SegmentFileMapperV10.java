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
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link SegmentFileMapper} implementation for V10 segment files.
 * <p>
 * V10 file format:
 * | version (byte) | meta compression (byte) | meta length (int) | meta json | chunk 0 | chunk 1 | ... | chunk n |
 */
public class SegmentFileMapperV10 implements SegmentFileMapper
{
  /**
   * Create a v10 {@link SegmentFileMapper} with 'external' attached v10 segment files
   *
   * @param segmentFile v10 segment file with name {@link IndexIO#V10_FILE_NAME}
   * @param mapper      json mapper to deserialize metadata
   * @param externals   list of 'external' v10 segment files to attach to this mapper and files that can be referenced
   *                    using {@link #mapExternalFile(String, String)}
   * @return v10 {@link SegmentFileMapper} using memory mapped {@link ByteBuffer}
   * @throws IOException
   */
  public static SegmentFileMapperV10 create(
      File segmentFile,
      ObjectMapper mapper,
      List<String> externals
  ) throws IOException
  {
    final SegmentFileMapperV10 entryPoint = create(segmentFile, mapper);

    final Map<String, SegmentFileMapperV10> externalMappers = new HashMap<>();
    try {
      for (String filename : externals) {
        final File externalFile = new File(segmentFile.getParentFile(), filename);
        if (externalFile.exists()) {
          externalMappers.put(filename, create(externalFile, mapper));
        }
      }
    }
    catch (Throwable t) {
      Closer closer = Closer.create();
      closer.registerAll(externalMappers.values());
      throw CloseableUtils.closeAndWrapInCatch(t, closer);
    }

    return new SegmentFileMapperV10(
        entryPoint.segmentFile,
        entryPoint.segmentFileMetadata,
        entryPoint.containers,
        externalMappers
    );
  }

  /**
   * Create a v10 {@link SegmentFileMapper}
   */
  public static SegmentFileMapperV10 create(
      File segmentFile,
      ObjectMapper mapper
  ) throws IOException
  {
    try (FileInputStream fis = new FileInputStream(segmentFile)) {
      // version (byte) | metadata compression (byte) | metadata length (int)
      byte[] header = new byte[1 + 1 + Integer.BYTES];
      int read = fis.read(header);
      if (read < header.length) {
        throw DruidException.defensive("expected at least [%s] bytes, but only read [%s]", header.length, read);
      }
      ByteBuffer headerBuffer = ByteBuffer.wrap(header);
      headerBuffer.order(ByteOrder.LITTLE_ENDIAN);

      if (headerBuffer.get(0) != IndexIO.V10_VERSION) {
        throw DruidException.defensive("not v10, got[%s] instead", headerBuffer.get(0));
      }

      // ideally we should make compression work, right now only uncompressed is supported (we probably need to add
      // another int for compressed length if strategy is to be compressed)
      byte compression = headerBuffer.get(1);
      CompressionStrategy compressionStrategy = CompressionStrategy.forId(compression);
      if (!CompressionStrategy.NONE.equals(compressionStrategy)) {
        throw DruidException.defensive("compression strategy[%s] not supported", compressionStrategy);
      }
      int metaLength = headerBuffer.getInt(2);

      byte[] meta = new byte[metaLength];
      read = fis.read(meta);
      if (read < meta.length) {
        throw DruidException.defensive("read[%s] which is less than expected metadata length[%s]", read, metaLength);
      }
      final int startOffset = header.length + meta.length;
      final SegmentFileMetadata metadata = mapper.readValue(meta, SegmentFileMetadata.class);
      final List<MappedByteBuffer> containers = Lists.newArrayListWithCapacity(metadata.getContainers().size());

      // eagerly map all container buffers so we can ensure they all share the same file descriptor without needing to
      // maintain an open channel (which could be closed during an interrupt for example)
      try (RandomAccessFile f = new RandomAccessFile(segmentFile, "r");
           FileChannel channel = f.getChannel()) {
        for (SmooshContainerMetadata containerMetadata : metadata.getContainers()) {
          containers.add(
              channel.map(
                  FileChannel.MapMode.READ_ONLY,
                  startOffset + containerMetadata.getStartOffset(),
                  containerMetadata.getSize()
              )
          );
        }
      }
      catch (IOException e) {
        Closer closer = Closer.create();
        for (MappedByteBuffer buffer : containers) {
          closer.register(() -> ByteBufferUtils.unmap(buffer));
        }
        CloseableUtils.closeAndWrapExceptions(closer);
        throw DruidException.defensive(e, "Problem mapping segment file[%s]", segmentFile.getAbsolutePath());
      }

      return new SegmentFileMapperV10(
          segmentFile,
          metadata,
          List.copyOf(containers),
          Map.of()
      );
    }
  }

  private final File segmentFile;
  private final SegmentFileMetadata segmentFileMetadata;
  private final List<MappedByteBuffer> containers;
  private final Map<String, SegmentFileMapperV10> externalMappers;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public SegmentFileMapperV10(
      final File segmentFile,
      final SegmentFileMetadata segmentFileMetadata,
      final List<MappedByteBuffer> containers,
      final Map<String, SegmentFileMapperV10> externalMappers
  )
  {
    this.segmentFile = segmentFile;
    this.segmentFileMetadata = segmentFileMetadata;
    this.containers = containers;
    this.externalMappers = externalMappers;
  }

  public SegmentFileMetadata getSegmentFileMetadata()
  {
    return segmentFileMetadata;
  }

  @Override
  public Set<String> getInternalFilenames()
  {
    return segmentFileMetadata.getFiles().keySet();
  }

  @Override
  @Nullable
  public ByteBuffer mapFile(String name) throws IOException
  {
    checkClosed();
    final SmooshFileMetadata fileMetadata = segmentFileMetadata.getFiles().get(name);
    if (fileMetadata == null) {
      return null;
    }
    final MappedByteBuffer container = containers.get(fileMetadata.getContainer());
    if (container == null) {
      throw DruidException.defensive("invalid container[%s]", fileMetadata.getContainer());
    }
    final ByteBuffer view = container.asReadOnlyBuffer();
    view.position(Ints.checkedCast(fileMetadata.getStartOffset()))
        .limit(Ints.checkedCast(fileMetadata.getStartOffset() + fileMetadata.getSize()));
    return view.slice();
  }

  @Override
  @Nullable
  public ByteBuffer mapExternalFile(String filename, String name) throws IOException
  {
    checkClosed();
    final SegmentFileMapperV10 externalMapper = externalMappers.get(filename);
    if (externalMapper == null) {
      throw DruidException.defensive("external file[%s] containing[%s] not found", filename, name);
    }
    return externalMapper.mapFile(name);
  }

  private void checkClosed()
  {
    if (closed.get()) {
      throw DruidException.defensive("Segment file[%s] is closed", segmentFile);
    }
  }

  @Override
  public void close()
  {
    if (closed.compareAndSet(false, true)) {
      Closer closer = Closer.create();
      for (MappedByteBuffer buffer : containers) {
        closer.register(() -> ByteBufferUtils.unmap(buffer));
      }
      closer.registerAll(externalMappers.values());
      CloseableUtils.closeAndWrapExceptions(closer);
    }
  }
}
