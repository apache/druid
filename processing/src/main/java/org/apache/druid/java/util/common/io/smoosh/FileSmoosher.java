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

package org.apache.druid.java.util.common.io.smoosh;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.MappedByteBufferHandler;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.file.SegmentFileBuilder;
import org.apache.druid.segment.file.SegmentFileChannel;
import org.apache.druid.segment.file.SmooshContainerMetadata;
import org.apache.druid.segment.file.SmooshFileMetadata;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A class that concatenates files together into configurable sized chunks,
 * works in conjunction with the SmooshedFileMapper to provide access to the
 * individual files.
 * <p/>
 * It does not split input files among separate output files, instead the
 * various "chunk" files will be varying sizes and it is not possible to add a
 * file of size greater than Integer.MAX_VALUE.
 * <p/>
 * This class is not thread safe.
 * <p/>
 * This class allows writing multiple files even if main
 * smoosh file writer is open. If main smoosh file writer is already open, it
 * delegates the write into temporary file on the file system which is later
 * copied on to the main smoosh file and underlying temporary file will be
 * cleaned up.
 */
public class FileSmoosher implements SegmentFileBuilder
{
  private static final String FILE_EXTENSION = "smoosh";
  private static final Joiner JOINER = Joiner.on(",");
  private static final Logger LOG = new Logger(FileSmoosher.class);

  private final File baseDir;
  private final int maxChunkSize;

  private final List<File> outFiles = new ArrayList<>();
  private final Map<String, Metadata> internalFiles = new TreeMap<>();
  // list of files completed writing content using delegated smooshedWriter.
  private List<File> completedFiles = new ArrayList<>();
  // list of files in process writing content using delegated smooshedWriter.
  private List<File> filesInProcess = new ArrayList<>();
  // delegated smooshedWriter creates a new temporary file per file added. we use a counter to name these temporary
  // files, and map the file number to the file name so we don't have to escape the file names (e.g. names with '/')
  private AtomicLong delegateFileCounter = new AtomicLong(0);
  private Map<String, String> delegateFileNameMap;

  private Outer currOut = null;
  private boolean writerCurrentlyInUse = false;

  // helper for SegmentFileBuilderV10 to have control over naming of smoosh output files; if this is non-null
  // meta.smoosh is not written
  @Nullable
  private final String outputFileName;

  public FileSmoosher(
      File baseDir
  )
  {
    this(baseDir, Integer.MAX_VALUE, null);
  }

  public FileSmoosher(
      File baseDir,
      int maxChunkSize
  )
  {
    this(baseDir, maxChunkSize, null);
  }

  public FileSmoosher(
      File baseDir,
      int maxChunkSize,
      @Nullable String outputFileName
  )
  {
    this.baseDir = baseDir;
    this.maxChunkSize = maxChunkSize;
    this.outputFileName = outputFileName;
    this.delegateFileNameMap = new HashMap<>();

    Preconditions.checkArgument(maxChunkSize > 0, "maxChunkSize must be a positive value.");
  }

  static File metaFile(File baseDir)
  {
    return new File(baseDir, StringUtils.format("meta.%s", FILE_EXTENSION));
  }

  static File makeChunkFile(File baseDir, int i)
  {
    return new File(baseDir, StringUtils.format("%05d.%s", i, FILE_EXTENSION));
  }

  static File makeChunkFile(File baseDir, String prefix, int i)
  {
    return new File(baseDir, StringUtils.format("%s-%05d.%s", prefix, i, FILE_EXTENSION));
  }

  public List<File> getOutFiles()
  {
    return outFiles;
  }

  public List<SmooshContainerMetadata> getContainers()
  {
    List<SmooshContainerMetadata> smooshContainers = new ArrayList<>();
    long offset = 0;
    for (File f : outFiles) {
      smooshContainers.add(new SmooshContainerMetadata(offset, f.length()));
      offset += f.length();
    }
    return smooshContainers;
  }

  public Map<String, SmooshFileMetadata> getInternalFiles()
  {
    Map<String, SmooshFileMetadata> smooshFileMetadata = new TreeMap<>();
    for (Map.Entry<String, Metadata> entry : internalFiles.entrySet()) {
      smooshFileMetadata.put(
          entry.getKey(),
          new SmooshFileMetadata(
              entry.getValue().getFileNum(),
              entry.getValue().getStartOffset(),
              entry.getValue().getEndOffset() - entry.getValue().getStartOffset()
          )
      );
    }
    return smooshFileMetadata;
  }

  @Override
  public void addColumn(String name, ColumnDescriptor columnDescriptor)
  {
    throw DruidException.defensive("not supported");
  }

  @Override
  public void add(String name, File fileToAdd) throws IOException
  {
    try (MappedByteBufferHandler fileMappingHandler = FileUtils.map(fileToAdd)) {
      add(name, fileMappingHandler.get());
    }
  }

  @Override
  public void add(String name, ByteBuffer bufferToAdd) throws IOException
  {
    if (name.contains(",")) {
      throw new IAE("Cannot have a comma in the name of a file, got[%s].", name);
    }

    if (internalFiles.get(name) != null) {
      throw new IAE("Cannot add files of the same name, already have [%s]", name);
    }

    long size = 0;
    size += bufferToAdd.remaining();

    try (SegmentFileChannel out = addWithChannel(name, size)) {
      out.write(bufferToAdd);
    }
  }

  @Override
  public SegmentFileChannel addWithChannel(final String name, final long size) throws IOException
  {
    return addWithSmooshedWriter(name, size);
  }

  public SmooshedWriter addWithSmooshedWriter(final String name, final long size) throws IOException
  {
    if (size > maxChunkSize) {
      throw DruidException.forPersona(DruidException.Persona.ADMIN)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build("Serialized buffer size[%,d] for column[%s] exceeds the maximum[%,d]. "
                                  + "Consider adjusting the tuningConfig - for example, reduce maxRowsPerSegment, "
                                  + "or partition your data further.",
                                  size, name, maxChunkSize
                          );
    }

    // If current writer is in use then create a new SmooshedWriter which
    // writes into temporary file which is later merged into original
    // FileSmoosher.
    if (writerCurrentlyInUse) {
      return delegateSmooshedWriter(name, size);
    }

    if (currOut == null) {
      currOut = getNewCurrOut();
    }
    if (currOut.bytesLeft() < size) {
      currOut.close();
      currOut = getNewCurrOut();
    }

    final int startOffset = currOut.getCurrOffset();
    writerCurrentlyInUse = true;
    return new SmooshedWriter()
    {
      private boolean open = true;
      private long bytesWritten = 0;

      @Override
      public int write(ByteBuffer in) throws IOException
      {
        return verifySize(currOut.write(in));
      }

      @Override
      public long write(ByteBuffer[] srcs, int offset, int length) throws IOException
      {
        return verifySize(currOut.write(srcs, offset, length));
      }

      @Override
      public long write(ByteBuffer[] srcs) throws IOException
      {
        return verifySize(currOut.write(srcs));
      }

      private int verifySize(long bytesWrittenInChunk)
      {
        bytesWritten += bytesWrittenInChunk;

        if (bytesWritten != currOut.getCurrOffset() - startOffset) {
          throw new ISE("Perhaps there is some concurrent modification going on?");
        }
        if (bytesWritten > size) {
          throw new ISE("Wrote[%,d] bytes for something of size[%,d].  Liar!!!", bytesWritten, size);
        }

        return Ints.checkedCast(bytesWrittenInChunk);
      }

      @Override
      public boolean isOpen()
      {
        return open;
      }

      @Override
      public void close() throws IOException
      {
        if (open) {
          open = false;
          internalFiles.put(name, new Metadata(currOut.getFileNum(), startOffset, currOut.getCurrOffset()));
          writerCurrentlyInUse = false;

          if (bytesWritten != currOut.getCurrOffset() - startOffset) {
            throw new ISE("Perhaps there is some concurrent modification going on?");
          }
          if (bytesWritten != size) {
            throw new IOE("Expected [%,d] bytes, only saw [%,d], potential corruption?", size, bytesWritten);
          }
          // Merge temporary files on to the main smoosh file.
          mergeWithSmoosher();
        }
      }
    };
  }

  /**
   * Merges temporary files created by delegated SmooshedWriters on to the main
   * smoosh file.
   *
   * @throws IOException
   */
  private void mergeWithSmoosher() throws IOException
  {
    // Get processed elements from the stack and write.
    List<File> fileToProcess = new ArrayList<>(completedFiles);
    Map<String, String> fileNameMap = ImmutableMap.copyOf(delegateFileNameMap);
    completedFiles = new ArrayList<>();
    delegateFileNameMap = new HashMap<>();
    for (File file : fileToProcess) {
      add(fileNameMap.get(file.getName()), file);
      if (!file.delete()) {
        LOG.warn("Unable to delete file [%s]", file);
      }
    }
  }

  /**
   * Returns a new SmooshedWriter which writes into temporary file and close
   * method on returned SmooshedWriter tries to merge temporary file into
   * original FileSmoosher object(if not open).
   *
   * @param name fileName
   * @param size size of the file.
   *
   * @return
   *
   * @throws IOException
   */
  private SmooshedWriter delegateSmooshedWriter(final String name, final long size) throws IOException
  {
    final String delegateName = getDelegateFileName(name);
    final File tmpFile = new File(baseDir, delegateName);
    filesInProcess.add(tmpFile);

    return new SmooshedWriter()
    {
      private final GatheringByteChannel channel =
          FileChannel.open(
              tmpFile.toPath(),
              StandardOpenOption.WRITE,
              StandardOpenOption.CREATE,
              StandardOpenOption.TRUNCATE_EXISTING
          );

      private int currOffset = 0;

      @Override
      public void close() throws IOException
      {
        channel.close();
        completedFiles.add(tmpFile);
        filesInProcess.remove(tmpFile);

        if (!writerCurrentlyInUse) {
          mergeWithSmoosher();
        }
      }

      public int bytesLeft()
      {
        return (int) (size - currOffset);
      }

      @Override
      public int write(ByteBuffer buffer) throws IOException
      {
        return addToOffset(channel.write(buffer));
      }

      @Override
      public long write(ByteBuffer[] srcs, int offset, int length) throws IOException
      {
        return addToOffset(channel.write(srcs, offset, length));
      }

      @Override
      public long write(ByteBuffer[] srcs) throws IOException
      {
        return addToOffset(channel.write(srcs));
      }

      public int addToOffset(long numBytesWritten)
      {
        if (numBytesWritten > bytesLeft()) {
          throw new ISE("Wrote more bytes[%,d] than available[%,d]. Don't do that.", numBytesWritten, bytesLeft());
        }
        currOffset += numBytesWritten;

        return Ints.checkedCast(numBytesWritten);
      }

      @Override
      public boolean isOpen()
      {
        return channel.isOpen();
      }
    };
  }

  private String getDelegateFileName(String name)
  {
    final String delegateName = String.valueOf(delegateFileCounter.getAndIncrement());
    delegateFileNameMap.put(delegateName, name);
    return delegateName;
  }

  @Override
  public void close() throws IOException
  {
    //book keeping checks on created file.
    if (!completedFiles.isEmpty() || !filesInProcess.isEmpty()) {
      for (File file : completedFiles) {
        if (!file.delete()) {
          LOG.warn("Unable to delete file [%s]", file);
        }
      }
      for (File file : filesInProcess) {
        if (!file.delete()) {
          LOG.warn("Unable to delete file [%s]", file);
        }
      }
      throw new ISE(
          "[%d] writers in progress and [%d] completed writers needs to be closed before closing smoosher.",
          filesInProcess.size(), completedFiles.size()
      );
    }

    if (currOut != null) {
      currOut.close();
    }

    if (outputFileName == null) {
      File metaFile = metaFile(baseDir);

      try (Writer out =
               new BufferedWriter(new OutputStreamWriter(new FileOutputStream(metaFile), StandardCharsets.UTF_8))) {
        out.write(StringUtils.format("v1,%d,%d", maxChunkSize, outFiles.size()));
        out.write("\n");

        for (Map.Entry<String, Metadata> entry : internalFiles.entrySet()) {
          final Metadata metadata = entry.getValue();
          out.write(
              JOINER.join(
                  entry.getKey(),
                  metadata.getFileNum(),
                  metadata.getStartOffset(),
                  metadata.getEndOffset()
              )
          );
          out.write("\n");
        }
      }
    }
  }

  private Outer getNewCurrOut() throws FileNotFoundException
  {
    final int fileNum = outFiles.size();
    File outFile = outputFileName != null
                   ? makeChunkFile(baseDir, outputFileName, fileNum)
                   : makeChunkFile(baseDir, fileNum);
    outFiles.add(outFile);
    return new Outer(fileNum, outFile, maxChunkSize);
  }

  public static class Outer implements SmooshedWriter
  {
    private final int fileNum;
    private final int maxLength;
    private final File outFile;
    private final GatheringByteChannel channel;

    private final Closer closer = Closer.create();
    private int currOffset = 0;

    Outer(int fileNum, File outFile, int maxLength) throws FileNotFoundException
    {
      this.fileNum = fileNum;
      this.outFile = outFile;
      this.maxLength = maxLength;

      FileOutputStream outStream = closer.register(new FileOutputStream(outFile));  // lgtm [java/output-resource-leak]
      this.channel = closer.register(outStream.getChannel());
    }

    public int getFileNum()
    {
      return fileNum;
    }

    public int getCurrOffset()
    {
      return currOffset;
    }

    public int bytesLeft()
    {
      return maxLength - currOffset;
    }

    @Override
    public int write(ByteBuffer buffer) throws IOException
    {
      return addToOffset(channel.write(buffer));
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException
    {
      return addToOffset(channel.write(srcs, offset, length));
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException
    {
      return addToOffset(channel.write(srcs));
    }

    public int addToOffset(long numBytesWritten)
    {
      if (numBytesWritten > bytesLeft()) {
        throw new ISE("Wrote more bytes[%,d] than available[%,d]. Don't do that.", numBytesWritten, bytesLeft());
      }
      currOffset += numBytesWritten;

      return Ints.checkedCast(numBytesWritten);
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
        LOG.debug("Created smoosh file [%s] of size [%s] bytes.", outFile.getAbsolutePath(), outFile.length());
      }
    }
  }
}
