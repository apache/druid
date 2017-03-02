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

package io.druid.segment.data;

import com.google.common.collect.Maps;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 */
public class TmpFileIOPeon implements IOPeon
{
  private final File dir;
  private final boolean allowOverwrite;
  private final Map<String, File> createdFiles = Maps.newLinkedHashMap();

  public TmpFileIOPeon()
  {
    this(true);
  }

  public TmpFileIOPeon(boolean allowOverwrite)
  {
    this(null, allowOverwrite);
  }

  public TmpFileIOPeon(File dir, boolean allowOverwrite)
  {
    this.dir = dir;
    this.allowOverwrite = allowOverwrite;
  }

  @Override
  public OutputStream makeOutputStream(String filename) throws IOException
  {
    File retFile = createdFiles.get(filename);
    if (retFile == null) {
      retFile = File.createTempFile("filePeon", filename, dir);
      createdFiles.put(filename, retFile);
      return new BufferedOutputStream(new FileOutputStream(retFile));
    } else if (allowOverwrite) {
      return new BufferedOutputStream(new FileOutputStream(retFile));
    } else {
      throw new IOException("tmp file conflicts, file[" + filename + "] already exist!");
    }
  }

  @Override
  public InputStream makeInputStream(String filename) throws IOException
  {
    final File retFile = createdFiles.get(filename);

    return retFile == null ? null : new FileInputStream(retFile);
  }

  @Override
  public void close() throws IOException
  {
    for (File file : createdFiles.values()) {
      file.delete();
    }
    createdFiles.clear();
  }

  public boolean isOverwriteAllowed()
  {
    return allowOverwrite;
  }

  @Override
  public File getFile(String filename)
  {
    return createdFiles.get(filename);
  }

}
