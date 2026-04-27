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
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.projections.ProjectionMetadata;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * {@link SegmentFileBuilder} for V10 format segments. Files are written into 'container' chunk files in {@link #baseDir}
 * and are concatenated after the header and {@link SegmentFileMetadata} on {@link #close()} to produce the final
 * consolidated segment file.
 * <p>
 * V10 file format:
 * | version (byte) | meta compression (byte) | meta length (int) | meta json | container 0 | ... | container n |
 * <p>
 * Containers are scoped to at most one declared file group. Callers declare which group they are writing via
 * {@link #startFileGroup(String)} before writing its files; a new container is started when the declared group
 * changes or the current container would exceed {@link #maxContainerSize}. A group whose total size exceeds the max
 * container size spans multiple containers, all tagged with the same group. This gives readers a clean 1:1 (or 1:N)
 * mapping between groups and containers, which supports per-group partial loading without any read-side reorganization.
 * Projections are the primary caller today, but the mechanism is equally usable for other organizational needs
 * (shared data across columns, internal metadata, etc.).
 * <p>
 * Callers that never invoke {@link #startFileGroup(String)} are mapped to a null-group container.
 * <p>
 * Much of the logic here was ported from {@link org.apache.druid.java.util.common.io.smoosh.FileSmoosher} of the V9
 * format and there is a fair bit of overlap. In fact, the initial implementation of this class wrapped a V9 smoosher
 * to build the files before combining them into the V10 format. The main difference is that V9 fills each container to
 * the max while here we organize with file groups.
 */
public class SegmentFileBuilderV10 implements SegmentFileBuilder
{
  private static final Logger LOG = new Logger(SegmentFileBuilderV10.class);

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
  private final long maxContainerSize;
  private final CompressionStrategy metadataCompression;
  private final Map<String, SegmentFileBuilderV10> externalSegmentFileBuilders;
  private final Map<String, ColumnDescriptor> columns = new TreeMap<>();

  private final List<ContainerWriter> containers = new ArrayList<>();
  private final Map<String, SegmentInternalFileMetadata> internalFiles = new TreeMap<>();

  // Nested addWithChannel calls (for example a serializer that, while being written, emits sub-files for its own
  // columnar parts) can't write into the current container concurrently with the outer writer. These nested writes are
  // redirected to temporary files and merged back into container(s) once the outer writer completes. Each entry
  // carries the file group that was active when the delegate was created so that the merge routes it into the
  // correct container even if the active group has since changed.
  private final List<DelegateEntry> completedDelegates = new ArrayList<>();
  private final List<DelegateEntry> inProgressDelegates = new ArrayList<>();
  private long delegateFileCounter = 0;

  @Nullable
  private ContainerWriter currentContainer = null;
  private boolean writerCurrentlyInUse = false;
  // The file group declared by the most recent {@link #startFileGroup} call. Writes are routed into containers
  // tagged with this group. Remains {@code null} if the caller never declares one, in which case all writes share
  // a single null-group container.
  @Nullable
  private String currentFileGroup = null;

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
      long maxContainerSize,
      CompressionStrategy metadataCompression
  )
  {
    this.jsonMapper = jsonMapper;
    this.outputFileName = outputFileName;
    this.baseDir = baseDir;
    this.maxContainerSize = maxContainerSize;
    this.metadataCompression = metadataCompression;
    this.externalSegmentFileBuilders = new TreeMap<>();
  }

  @Override
  public void add(String name, File fileToAdd) throws IOException
  {
    try (FileInputStream fis = new FileInputStream(fileToAdd);
         FileChannel src = fis.getChannel()) {
      final long size = src.size();
      try (SegmentFileChannel out = addWithChannel(name, size)) {
        long position = 0;
        while (position < size) {
          final long transferred = src.transferTo(position, size - position, out);
          if (transferred <= 0) {
            throw new IOE("Unable to transfer bytes from file[%s] at position[%,d]", fileToAdd, position);
          }
          position += transferred;
        }
      }
    }
  }

  @Override
  public void add(String name, ByteBuffer bufferToAdd) throws IOException
  {
    try (SegmentFileChannel out = addWithChannel(name, bufferToAdd.remaining())) {
      out.write(bufferToAdd);
    }
  }

  @Override
  public SegmentFileChannel addWithChannel(final String name, final long size) throws IOException
  {
    if (name.contains(",")) {
      throw new IAE("Cannot have a comma in the name of a file, got[%s].", name);
    }
    if (internalFiles.containsKey(name)) {
      throw new IAE("Cannot add files of the same name, already have [%s]", name);
    }
    if (size > maxContainerSize) {
      throw DruidException.forPersona(DruidException.Persona.ADMIN)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build(
                              "Serialized buffer size[%,d] for column[%s] exceeds the maximum[%,d]. "
                              + "Consider adjusting the tuningConfig - for example, reduce maxRowsPerSegment, "
                              + "or partition your data further.",
                              size, name, maxContainerSize
                          );
    }

    // If an outer writer is mid-write we can't append to the current container concurrently, route through a temp
    // file that will be merged back into a container once the outer writer releases.
    if (writerCurrentlyInUse) {
      return delegateChannel(name, size);
    }

    ensureContainer(currentFileGroup, size);
    final ContainerWriter target = currentContainer;
    final long startOffset = target.currOffset;
    writerCurrentlyInUse = true;

    return new SegmentFileChannel()
    {
      private boolean open = true;
      private long bytesWritten = 0;

      @Override
      public int write(ByteBuffer src) throws IOException
      {
        return Ints.checkedCast(verifySize(target.write(src)));
      }

      @Override
      public long write(ByteBuffer[] srcs, int offset, int length) throws IOException
      {
        return verifySize(target.write(srcs, offset, length));
      }

      @Override
      public long write(ByteBuffer[] srcs) throws IOException
      {
        return verifySize(target.write(srcs));
      }

      private long verifySize(long bytesWrittenInChunk)
      {
        bytesWritten += bytesWrittenInChunk;

        if (bytesWritten != target.currOffset - startOffset) {
          throw new ISE("Perhaps there is some concurrent modification going on?");
        }
        if (bytesWritten > size) {
          throw new ISE("Wrote[%,d] bytes for something of size[%,d].  Liar!!!", bytesWritten, size);
        }

        return bytesWrittenInChunk;
      }

      @Override
      public boolean isOpen()
      {
        return open;
      }

      @Override
      public void close() throws IOException
      {
        if (!open) {
          return;
        }
        open = false;
        writerCurrentlyInUse = false;

        if (bytesWritten != target.currOffset - startOffset) {
          throw new ISE("Perhaps there is some concurrent modification going on?");
        }
        if (bytesWritten != size) {
          throw new IOE("Expected [%,d] bytes, only saw [%,d], potential corruption?", size, bytesWritten);
        }
        internalFiles.put(
            name,
            new SegmentInternalFileMetadata(target.fileNum, startOffset, target.currOffset - startOffset)
        );
        mergeDelegatedFiles();
      }
    };
  }

  @Override
  public SegmentFileBuilder getExternalBuilder(String externalFile)
  {
    return externalSegmentFileBuilders.computeIfAbsent(
        externalFile,
        (k) -> new SegmentFileBuilderV10(jsonMapper, externalFile, baseDir, maxContainerSize, metadataCompression)
    );
  }

  @Override
  public void addColumn(String name, ColumnDescriptor columnDescriptor)
  {
    this.columns.put(name, columnDescriptor);
  }

  /**
   * Declare the file group that subsequent writes belong to. Writes are routed into a container tagged with the
   * declared group; a new container is rolled when the group changes or the incoming file won't fit. A group whose
   * total size exceeds {@link #maxContainerSize} is split across multiple consecutive containers, all tagged with
   * the same group. Passing {@code null} clears the current group; subsequent writes are then routed into a
   * null-group container until the next call.
   * <p>
   * Current V10-specific limitations worth knowing:
   * <ul>
   *   <li>Groups cannot be re-entered. Once a different group (or {@code null}) has been declared, the previous
   *       group's container is closed, and you cannot go back and append more files to it, any such writes would
   *       open a fresh container for the re-declared group, so the group's files would end up in non-contiguous
   *       containers. If all of a group's files must land in the same container(s), write them contiguously.</li>
   *   <li>Throws if called while a writer returned by {@link #addWithChannel} is still open.</li>
   * </ul>
   */
  @Override
  public void startFileGroup(@Nullable String groupName)
  {
    if (writerCurrentlyInUse) {
      throw DruidException.defensive("Cannot start file group[%s] while a writer is in progress", groupName);
    }
    this.currentFileGroup = groupName;
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
    if (currentContainer != null) {
      CloseableUtils.closeAndWrapExceptions(currentContainer);
    }
  }

  @Override
  public void close() throws IOException
  {
    for (SegmentFileBuilderV10 externalBuilder : externalSegmentFileBuilders.values()) {
      externalBuilder.close();
    }

    if (!completedDelegates.isEmpty() || !inProgressDelegates.isEmpty()) {
      abort();
      throw new ISE(
          "[%d] writers in progress and [%d] completed writers needs to be closed before closing builder.",
          inProgressDelegates.size(),
          completedDelegates.size()
      );
    }

    if (currentContainer != null) {
      currentContainer.close();
    }

    final SegmentFileMetadata segmentFileMetadata = new SegmentFileMetadata(
        buildContainerMetadata(),
        internalFiles,
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

      // compress the data if specified, however we throw it out if compression is larger than uncompressed
      final ByteBuffer compressed;
      if (CompressionStrategy.NONE != metadataCompression) {
        // compress the data using the strategy, write the compressed length, then the compressed blob
        final CompressionStrategy.Compressor compressor = metadataCompression.getCompressor();
        final ByteBuffer inBuffer = compressor.allocateInBuffer(metadataBytes.length, closer)
                                              .order(ByteOrder.nativeOrder());
        inBuffer.put(metadataBytes, 0, metadataBytes.length);
        inBuffer.flip();

        final ByteBuffer outBuffer = compressor.allocateOutBuffer(metadataBytes.length, closer)
                                               .order(ByteOrder.nativeOrder());
        compressed = compressor.compress(inBuffer, outBuffer);
      } else {
        compressed = null;
      }
      final boolean shouldCompress = compressed != null && (4 + compressed.remaining()) < metadataBytes.length;

      outputStream.write(new byte[]{
          IndexIO.V10_VERSION,
          shouldCompress ? metadataCompression.getId() : CompressionStrategy.NONE.getId()
      });
      // write uncompressed metadata length
      intBuffer.putInt(metadataBytes.length);
      intBuffer.flip();
      outputStream.write(intBuffer.array());

      if (CompressionStrategy.NONE == metadataCompression || !shouldCompress) {
        // no compression, just write the plain metadata bytes
        outputStream.write(metadataBytes);
      } else {
        // write compression length
        intBuffer.position(0);
        intBuffer.putInt(compressed.remaining());
        intBuffer.flip();
        outputStream.write(intBuffer.array());

        // write compressed metadata
        Channels.writeFully(channel, compressed);
      }

      for (ContainerWriter container : containers) {
        final File f = container.file;
        try (FileInputStream fis = new FileInputStream(f)) {
          byte[] buffer = new byte[4096];
          int bytesRead;
          while ((bytesRead = fis.read(buffer)) != -1) {
            outputStream.write(buffer, 0, bytesRead);
          }
        }
        // delete all the old container files
        DruidException.conditionalDefensive(
            f.delete(),
            "Failed to delete temporary container file[%s]",
            f
        );
      }
    }
  }

  private List<SegmentFileContainerMetadata> buildContainerMetadata()
  {
    final List<SegmentFileContainerMetadata> result = new ArrayList<>(containers.size());
    long offset = 0;
    for (ContainerWriter container : containers) {
      final long length = container.file.length();
      result.add(new SegmentFileContainerMetadata(offset, length));
      offset += length;
    }
    return result;
  }

  /**
   * Ensure that {@link #currentContainer} is ready to accept {@code size} bytes of a file belonging to {@code group}.
   * Rolls the current container and starts a new one when:
   * <ul>
   *   <li>there is no current container, or</li>
   *   <li>the current container is for a different group, or</li>
   *   <li>the current container cannot fit the incoming bytes within {@link #maxContainerSize}.</li>
   * </ul>
   */
  private void ensureContainer(@Nullable String group, long size) throws IOException
  {
    if (currentContainer == null
        || !Objects.equals(currentContainer.group, group)
        || !currentContainer.canFit(size)) {
      if (currentContainer != null) {
        currentContainer.close();
      }
      currentContainer = openNewContainer(group);
      containers.add(currentContainer);
    }
  }

  private ContainerWriter openNewContainer(@Nullable String group) throws IOException
  {
    FileUtils.mkdirp(baseDir);
    final int fileNum = containers.size();
    final File containerFile = new File(
        baseDir,
        StringUtils.format("%s-%05d.container", outputFileName, fileNum)
    );
    return new ContainerWriter(fileNum, containerFile, group, maxContainerSize);
  }

  private SegmentFileChannel delegateChannel(final String name, final long size) throws IOException
  {
    // Prefixed with outputFileName so delegate files from a main builder and its externals (which share baseDir)
    // cannot collide, since main and external always have distinct output file names.
    final String delegateName = StringUtils.format("%s-delegate-%d", outputFileName, delegateFileCounter++);
    final File tmpFile = new File(baseDir, delegateName);
    // Snapshot the active group now so that if this delegate is merged after the outer writer has advanced past
    // the group it was created under, it still routes into the correct container.
    final DelegateEntry entry = new DelegateEntry(tmpFile, name, currentFileGroup);
    inProgressDelegates.add(entry);

    return new SegmentFileChannel()
    {
      private final FileChannel channel = FileChannel.open(
          tmpFile.toPath(),
          StandardOpenOption.WRITE,
          StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING
      );

      private long bytesWritten = 0;

      @Override
      public int write(ByteBuffer src) throws IOException
      {
        return Ints.checkedCast(addBytes(channel.write(src)));
      }

      @Override
      public long write(ByteBuffer[] srcs, int offset, int length) throws IOException
      {
        return addBytes(channel.write(srcs, offset, length));
      }

      @Override
      public long write(ByteBuffer[] srcs) throws IOException
      {
        return addBytes(channel.write(srcs));
      }

      private long addBytes(long n)
      {
        if (n > size - bytesWritten) {
          throw new ISE(
              "Wrote more bytes[%,d] than expected[%,d] for delegated file[%s]",
              bytesWritten + n, size, name
          );
        }
        bytesWritten += n;
        return n;
      }

      @Override
      public boolean isOpen()
      {
        return channel.isOpen();
      }

      @Override
      public void close() throws IOException
      {
        channel.close();
        completedDelegates.add(entry);
        inProgressDelegates.remove(entry);
        if (!writerCurrentlyInUse) {
          mergeDelegatedFiles();
        }
      }
    };
  }

  /**
   * Move completed delegate temp files into containers by replaying them as regular {@link #add} calls. Only called
   * when no outer writer is currently holding the builder. Each entry's snapshotted group is restored as
   * {@link #currentFileGroup} during its replay so the file lands in the container that was active when the
   * nested write was originally requested, not whichever group happens to be active at merge time.
   */
  private void mergeDelegatedFiles() throws IOException
  {
    if (completedDelegates.isEmpty()) {
      return;
    }
    final List<DelegateEntry> toProcess = new ArrayList<>(completedDelegates);
    completedDelegates.clear();
    final String savedGroup = currentFileGroup;
    try {
      for (DelegateEntry entry : toProcess) {
        currentFileGroup = entry.group;
        add(entry.name, entry.file);
        if (!entry.file.delete()) {
          LOG.warn("Unable to delete delegate file[%s]", entry.file);
        }
      }
    }
    finally {
      currentFileGroup = savedGroup;
    }
  }

  private record DelegateEntry(File file, String name, @Nullable String group)
  {
  }

  /**
   * Low-level writer for a single container chunk file. One container holds internal files from at most one group.
   */
  private static class ContainerWriter implements GatheringByteChannel
  {
    private final int fileNum;
    private final File file;
    @Nullable
    private final String group;
    private final long maxSize;
    private final Closer closer = Closer.create();
    private final GatheringByteChannel channel;
    private long currOffset = 0;

    ContainerWriter(int fileNum, File file, @Nullable String group, long maxSize) throws IOException
    {
      this.fileNum = fileNum;
      this.file = file;
      this.group = group;
      this.maxSize = maxSize;
      final FileOutputStream outStream = closer.register(new FileOutputStream(file));
      this.channel = closer.register(outStream.getChannel());
    }

    boolean canFit(long size)
    {
      // overflow-safe form of currOffset + size <= maxSize for non-negative currOffset/size/maxSize
      return size <= maxSize - currOffset;
    }

    @Override
    public int write(ByteBuffer src) throws IOException
    {
      return Ints.checkedCast(recordWrite(channel.write(src)));
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException
    {
      return recordWrite(channel.write(srcs, offset, length));
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException
    {
      return recordWrite(channel.write(srcs));
    }

    private long recordWrite(long n)
    {
      if (n > maxSize - currOffset) {
        throw new ISE("Wrote more bytes[%,d] than available[%,d]", n, maxSize - currOffset);
      }
      currOffset += n;
      return n;
    }

    @Override
    public boolean isOpen()
    {
      return channel.isOpen();
    }

    @Override
    public void close() throws IOException
    {
      closer.close();
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Created container file[%s] for group[%s] of size[%,d] bytes.",
            file.getAbsolutePath(),
            group,
            file.length()
        );
      }
    }
  }
}
