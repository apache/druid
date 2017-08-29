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

package io.druid.java.util.common.io.smoosh;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.FileUtils;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.IOE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.MappedByteBufferHandler;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.io.Closer;
import io.druid.java.util.common.logger.Logger;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
public class FileSmoosher implements Closeable
{
  private static final String FILE_EXTENSION = "smoosh";
  private static final Joiner joiner = Joiner.on(",");
  private static final Logger LOG = new Logger(FileSmoosher.class);

  private final File baseDir;
  private final int maxChunkSize;

  private final List<File> outFiles = Lists.newArrayList();
  private final Map<String, Metadata> internalFiles = Maps.newTreeMap();
  // list of files completed writing content using delegated smooshedWriter.
  private List<File> completedFiles = Lists.newArrayList();
  // list of files in process writing content using delegated smooshedWriter.
  private List<File> filesInProcess = Lists.newArrayList();

  private Outer currOut = null;
  private boolean writerCurrentlyInUse = false;

  public FileSmoosher(
      File baseDir
  )
  {
    this(baseDir, Integer.MAX_VALUE);
  }

  public FileSmoosher(
      File baseDir,
      int maxChunkSize
  )
  {
    this.baseDir = baseDir;
    this.maxChunkSize = maxChunkSize;

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

  public Set<String> getInternalFilenames()
  {
    return internalFiles.keySet();
  }

  public void add(File fileToAdd) throws IOException
  {
    add(fileToAdd.getName(), fileToAdd);
  }

  public void add(String name, File fileToAdd) throws IOException
  {
    try (MappedByteBufferHandler fileMappingHandler = FileUtils.map(fileToAdd)) {
      add(name, fileMappingHandler.get());
    }
  }

  public void add(String name, ByteBuffer bufferToAdd) throws IOException
  {
    add(name, Collections.singletonList(bufferToAdd));
  }

  public void add(String name, List<ByteBuffer> bufferToAdd) throws IOException
  {
    if (name.contains(",")) {
      throw new IAE("Cannot have a comma in the name of a file, got[%s].", name);
    }

    if (internalFiles.get(name) != null) {
      throw new IAE("Cannot add files of the same name, already have [%s]", name);
    }

    long size = 0;
    for (ByteBuffer buffer : bufferToAdd) {
      size += buffer.remaining();
    }

    try (SmooshedWriter out = addWithSmooshedWriter(name, size)) {
      for (ByteBuffer buffer : bufferToAdd) {
        out.write(buffer);
      }
    }
  }

  public SmooshedWriter addWithSmooshedWriter(final String name, final long size) throws IOException
  {

    if (size > maxChunkSize) {
      throw new IAE("Asked to add buffers[%,d] larger than configured max[%,d]", size, maxChunkSize);
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
      public int write(InputStream in) throws IOException
      {
        return verifySize(currOut.write(in));
      }

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

      private int verifySize(long bytesWrittenInChunk) throws IOException
      {
        bytesWritten += bytesWrittenInChunk;

        if (bytesWritten != currOut.getCurrOffset() - startOffset) {
          throw new ISE("WTF? Perhaps there is some concurrent modification going on?");
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
        open = false;
        internalFiles.put(name, new Metadata(currOut.getFileNum(), startOffset, currOut.getCurrOffset()));
        writerCurrentlyInUse = false;

        if (bytesWritten != currOut.getCurrOffset() - startOffset) {
          throw new ISE("WTF? Perhaps there is some concurrent modification going on?");
        }
        if (bytesWritten != size) {
          throw new IOE("Expected [%,d] bytes, only saw [%,d], potential corruption?", size, bytesWritten);
        }
        // Merge temporary files on to the main smoosh file.
        mergeWithSmoosher();
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
    completedFiles = Lists.newArrayList();
    for (File file : fileToProcess) {
      add(file);
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
    final File tmpFile = new File(baseDir, name);
    filesInProcess.add(tmpFile);

    return new SmooshedWriter()
    {
      private final FileOutputStream out = new FileOutputStream(tmpFile);
      private final GatheringByteChannel channel = out.getChannel();
      private final Closer closer = Closer.create();

      private int currOffset = 0;

      {
        closer.register(out);
        closer.register(channel);
      }

      @Override
      public void close() throws IOException
      {
        closer.close();
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
      public int write(InputStream in) throws IOException
      {
        return addToOffset(ByteStreams.copy(Channels.newChannel(in), channel));
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

    File metaFile = metaFile(baseDir);

    try (Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(metaFile), Charsets.UTF_8))) {
      out.write(StringUtils.format("v1,%d,%d", maxChunkSize, outFiles.size()));
      out.write("\n");

      for (Map.Entry<String, Metadata> entry : internalFiles.entrySet()) {
        final Metadata metadata = entry.getValue();
        out.write(
            joiner.join(
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

  private Outer getNewCurrOut() throws FileNotFoundException
  {
    final int fileNum = outFiles.size();
    File outFile = makeChunkFile(baseDir, fileNum);
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

      FileOutputStream outStream = closer.register(new FileOutputStream(outFile));
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
    public int write(InputStream in) throws IOException
    {
      return addToOffset(ByteStreams.copy(Channels.newChannel(in), channel));
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
      FileSmoosher.LOG.info("Created smoosh file [%s] of size [%s] bytes.", outFile.getAbsolutePath(), outFile.length());
    }
  }
}
