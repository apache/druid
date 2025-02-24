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

package org.apache.druid.query.groupby.epinephelinae;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.groupby.GroupByStatsProvider;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An area for limited temporary storage on disk. Limits are checked when opening files and on all writes to those
 * files. Thread-safe.
 */
public class LimitedTemporaryStorage implements Closeable
{
  private static final Logger log = new Logger(LimitedTemporaryStorage.class);

  private final GroupByStatsProvider.PerQueryStats perQueryStatsContainer;

  private final File storageDirectory;
  private final long maxBytesUsed;

  private final AtomicLong bytesUsed = new AtomicLong();
  private final Set<File> files = new TreeSet<>();

  private volatile boolean closed = false;

  private boolean createdStorageDirectory = false;

  public LimitedTemporaryStorage(
      File storageDirectory,
      long maxBytesUsed,
      GroupByStatsProvider.PerQueryStats perQueryStatsContainer
  )
  {
    this.storageDirectory = storageDirectory;
    this.maxBytesUsed = maxBytesUsed;
    this.perQueryStatsContainer = perQueryStatsContainer;
  }

  /**
   * Create a new temporary file. All methods of the returned output stream may throw
   * {@link TemporaryStorageFullException} if the temporary storage area fills up.
   *
   * @return output stream to the file
   *
   * @throws TemporaryStorageFullException if the temporary storage area is full
   * @throws IOException                   if something goes wrong while creating the file
   */
  public LimitedOutputStream createFile() throws IOException
  {
    if (bytesUsed.get() >= maxBytesUsed) {
      throw new TemporaryStorageFullException(maxBytesUsed);
    }

    synchronized (files) {
      if (closed) {
        throw new ISE("Closed");
      }

      FileUtils.mkdirp(storageDirectory);
      if (!createdStorageDirectory) {
        createdStorageDirectory = true;
      }

      final File theFile = new File(storageDirectory, StringUtils.format("%08d.tmp", files.size()));
      final EnumSet<StandardOpenOption> openOptions = EnumSet.of(
          StandardOpenOption.CREATE_NEW,
          StandardOpenOption.WRITE
      );

      final FileChannel channel = FileChannel.open(theFile.toPath(), openOptions);
      files.add(theFile);
      return new LimitedOutputStream(theFile, Channels.newOutputStream(channel));
    }
  }

  public void delete(final File file)
  {
    synchronized (files) {
      if (files.contains(file)) {
        try {
          Files.delete(file.toPath());
        }
        catch (IOException e) {
          log.warn(e, "Cannot delete file: %s", file);
        }
        files.remove(file);
      }
    }
  }

  public long maxSize()
  {
    return maxBytesUsed;
  }

  @VisibleForTesting
  public long currentSize()
  {
    return bytesUsed.get();
  }

  @Override
  public void close()
  {
    synchronized (files) {
      if (closed) {
        return;
      }
      closed = true;

      perQueryStatsContainer.spilledBytes(bytesUsed.get());

      bytesUsed.set(0);

      for (File file : ImmutableSet.copyOf(files)) {
        delete(file);
      }
      files.clear();
      if (createdStorageDirectory && storageDirectory.exists() && !storageDirectory.delete()) {
        log.warn("Cannot delete storageDirectory: %s", storageDirectory);
      }
    }
  }

  public class LimitedOutputStream extends OutputStream
  {
    private final File file;
    private final OutputStream out;

    private LimitedOutputStream(File file, OutputStream out)
    {
      this.file = file;
      this.out = out;
    }

    @Override
    public void write(int b) throws IOException
    {
      grab(1);
      out.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException
    {
      grab(b.length);
      out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException
    {
      grab(len);
      out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException
    {
      out.flush();
    }

    @Override
    public void close() throws IOException
    {
      out.close();
    }

    public File getFile()
    {
      return file;
    }

    private void grab(int n) throws IOException
    {
      if (bytesUsed.addAndGet(n) > maxBytesUsed) {
        throw new TemporaryStorageFullException(maxBytesUsed);
      }
    }
  }
}
