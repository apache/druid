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

package org.apache.druid.storage.remote;

import com.google.common.base.Predicates;
import org.apache.commons.io.IOUtils;
import org.apache.druid.data.input.impl.prefetch.ObjectOpenFunction;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class TestStorageConnector extends ChunkingStorageConnector<TestStorageConnector.InputRange>
{

  public static final String DATA = "This is some random data text. This should be returned in chunks by the methods, "
                                     + "however the connector should reassemble it as a single stream of text";

  public static final int CHUNK_SIZE_BYTES = 4;

  private final File tempDir;

  public TestStorageConnector(
      final File tempDir
  )
  {
    super(CHUNK_SIZE_BYTES);
    this.tempDir = tempDir;
  }

  @Override
  public ChunkingStorageConnectorParameters<TestStorageConnector.InputRange> buildInputParams(String path)
  {
    return buildInputParams(path, 0, DATA.length());
  }

  @Override
  public ChunkingStorageConnectorParameters<TestStorageConnector.InputRange> buildInputParams(
      String path,
      long from,
      long size
  )
  {
    ChunkingStorageConnectorParameters.Builder<InputRange> builder = new ChunkingStorageConnectorParameters.Builder<>();
    builder.start(from);
    builder.end(from + size);
    builder.cloudStoragePath(path);
    builder.tempDirSupplier(() -> tempDir);
    builder.retryCondition(Predicates.alwaysFalse());
    builder.maxRetry(2);
    builder.objectSupplier((start, end) -> new InputRange((int) start, (int) end));
    builder.objectOpenFunction(new ObjectOpenFunction<InputRange>()
    {
      @Override
      public InputStream open(InputRange ir)
      {
        return IOUtils.toInputStream(DATA.substring(ir.start, ir.end), StandardCharsets.UTF_8);
      }

      @Override
      public InputStream open(InputRange ir, long offset)
      {
        return open(new InputRange(ir.start + (int) offset, ir.end));
      }
    });
    return builder.build();
  }

  @Override
  public boolean pathExists(String path)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public OutputStream write(String path)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteFile(String path)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteFiles(Iterable<String> paths)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteRecursively(String path)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<String> listDir(String dirName)
  {
    throw new UnsupportedOperationException();
  }

  public static class InputRange
  {
    private final int start;
    private final int end;

    public InputRange(int start, int end)
    {
      this.start = start;
      this.end = end;
    }
  }
}
