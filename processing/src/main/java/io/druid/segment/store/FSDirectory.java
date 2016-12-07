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
package io.druid.segment.store;


import java.io.FilterOutputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * base class for local system Directory implementation
 */
public abstract class FSDirectory extends Directory
{

  protected final Path directory; // The underlying filesystem directory

  /**
   * create directory at the specific location
   *
   * @param path
   */
  protected FSDirectory(Path path) throws IOException
  {
    if (!Files.isDirectory(path)) {
      Files.createDirectories(path);
    }
    directory = path.toRealPath();
  }


  /**
   * Lists all files (including subdirectories) in the directory.
   *
   * @throws IOException if there was an I/O error during listing
   */
  public static String[] listAll(Path dir) throws IOException
  {
    return listAll(dir, null);
  }

  private static String[] listAll(Path dir, Set<String> skipNames) throws IOException
  {
    List<String> entries = new ArrayList<>();

    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
      for (Path path : stream) {
        String name = path.getFileName().toString();
        if (skipNames == null) {
          entries.add(name);
          continue;
        }
        if (skipNames != null && skipNames.contains(name) == false) {
          entries.add(name);
        }
      }
    }
    String[] array = entries.toArray(new String[entries.size()]);
    Arrays.sort(array);
    return array;
  }

  @Override
  public String[] listAll() throws IOException
  {
    return listAll(directory);
  }

  @Override
  public IndexOutput createOutput(String name) throws IOException
  {
    return new FSIndexOutput(name);
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix) throws IOException
  {
    String tmpName = prefix + UUID.randomUUID().toString() + suffix;

    return new FSIndexOutput(tmpName,
                             StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW
    );

  }


  @Override
  public void rename(String source, String dest) throws IOException
  {
    Files.move(directory.resolve(source), directory.resolve(dest), StandardCopyOption.ATOMIC_MOVE);
  }

  @Override
  public void close() throws IOException
  {
    super.refresh();
    Files.delete(directory);
  }

  /**
   * @return the underlying filesystem directory
   */
  public Path getDirectory()
  {
    return directory;
  }

  @Override
  public String toString()
  {
    return this.getClass().getSimpleName() + "@" + directory;
  }


  @Override
  public void deleteFile(String name) throws IOException
  {
    privateDeleteFile(name);
  }


  private void privateDeleteFile(String name) throws IOException
  {
    Files.delete(directory.resolve(name));
  }


  final class FSIndexOutput extends OutputStreamIndexOutput
  {
    /**
     * The maximum chunk size is 8192 bytes, because file channel mallocs
     * a native buffer outside of stack if the write buffer size is larger.
     */
    static final int CHUNK_SIZE = 8192;

    public FSIndexOutput(String name) throws IOException
    {
      this(name, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
    }

    FSIndexOutput(String name, OpenOption... options) throws IOException
    {
      super(
          name,
          new FilterOutputStream(Files.newOutputStream(directory.resolve(name), options))
          {
            // This implementation ensures, that we never write more than CHUNK_SIZE bytes:
            @Override
            public void write(byte[] b, int offset, int length) throws IOException
            {
              while (length > 0) {
                final int chunk = Math.min(length, CHUNK_SIZE);
                out.write(b, offset, chunk);
                length -= chunk;
                offset += chunk;
              }
            }
          },
          CHUNK_SIZE
      );
    }

  }
}
