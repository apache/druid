/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.offheap;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.collections.StupidPool;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A StupidPool which draws from an ever-increasing MemoryMapped file. It is assumed that each buffer drawn is of
 * equal size.
 */
public class MappedBufferPool extends StupidPool<ByteBuffer>
{
  private static final Logger log = new Logger(MappedBufferPool.class);

  public MappedBufferPool(final int computationBufferSize)
  {
    // This has its own class instead of just making a new Supplier<> because it also implements Closeable
    super(
        new FileChannelAppendableBufferPool(computationBufferSize)
    );
  }

  private static class FileChannelAppendableBufferPool implements Closeable, Supplier<ByteBuffer>
  {
    private final RandomAccessFile randomAccessFile;
    private final File tmpFile;
    private FileChannel currentChannel = null;
    private final AtomicInteger currentChannelBufferCount = new AtomicInteger(0);
    private final int computationBufferSize;

    FileChannelAppendableBufferPool(int computationBufferSize)
    {
      try {
        this.computationBufferSize = computationBufferSize;
        tmpFile = File.createTempFile("OffheapBufferPool", ".scratch");
        if (log.isDebugEnabled()) {
          log.debug("Buffer Opening %s", tmpFile.getCanonicalPath());
        }
        tmpFile.deleteOnExit();
        randomAccessFile = new RandomAccessFile(tmpFile, "rw");
        final FileChannel fileChannel = randomAccessFile.getChannel();
        this.currentChannel = fileChannel;
        currentChannelBufferCount.set(0);
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public synchronized ByteBuffer get()
    {
      try {
        return currentChannel.map(
            FileChannel.MapMode.READ_WRITE,
            currentChannelBufferCount.getAndIncrement() * computationBufferSize,
            computationBufferSize
        );
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    boolean closed = false;

    @Override
    public synchronized void close() throws IOException
    {
      if(closed){
        throw new ISE("Already closed");
      }
      IOException ex = null;
      try {
        // This closes the channel
        randomAccessFile.close();
      }
      catch (IOException e) {
        ex = e;
      }

      if (log.isDebugEnabled()) {
        log.debug("Buffer Closing %s of size %d", tmpFile.getCanonicalPath(), tmpFile.length());
      }
      tmpFile.delete();
      closed = true;
      if(ex != null){
        throw ex;
      }
    }

    @Override
    public void finalize() throws IOException
    {
      if (!closed) {
        close();
      }
    }
  }
}
