/*
 *
 *  Licensed to Metamarkets Group Inc. (Metamarkets) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. Metamarkets licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 * /
 *
 */

package org.mapdb;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is exclusively for use by OffheapIncrementalIndex. Only reason this class belongs in "org.mapdb"
 * package is for it to be able to access "protected" method on Volume.MappedFileVol .
 */
public class MappedFileVolWrapper extends Volume.ByteBufferVol
{
  private Volume.MappedFileVol delegate;
  private AtomicLong sizeAvailable;

  public MappedFileVolWrapper(File file, int chunkShift, AtomicLong sizeAvailable)
  {
    super(false, sizeAvailable.get(), chunkShift);
    this.delegate = new Volume.MappedFileVol(file, false, sizeAvailable.get(), chunkShift, 0);
    this.sizeAvailable = sizeAvailable;
  }

  @Override
  public ByteBuffer makeNewBuffer(long offset)
  {
    if (sizeAvailable.addAndGet(-1 * chunkSize) > 0) {
      return delegate.makeNewBuffer(offset);
    } else {
      sizeAvailable.addAndGet(chunkSize);
      throw new RuntimeException("No more space left for allocating buffers.");
    }
  }

  @Override
  public void truncate(long size)
  {
    delegate.truncate(size);
  }

  @Override
  public void close()
  {
    delegate.close();
  }

  @Override
  public void sync()
  {
  }

  @Override
  public void deleteFile()
  {
    delegate.deleteFile();
  }

  @Override
  public File getFile()
  {
    return delegate.getFile();
  }
}
