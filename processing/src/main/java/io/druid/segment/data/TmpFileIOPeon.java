/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
  Map<String, File> createdFiles = Maps.newLinkedHashMap();

  @Override
  public OutputStream makeOutputStream(String filename) throws IOException
  {
    File retFile = createdFiles.get(filename);
    if (retFile == null) {
      retFile = File.createTempFile("filePeon", filename);
      retFile.deleteOnExit();
      createdFiles.put(filename, retFile);
    }
    return new BufferedOutputStream(new FileOutputStream(retFile));
  }

  @Override
  public InputStream makeInputStream(String filename) throws IOException
  {
    final File retFile = createdFiles.get(filename);

    return retFile == null ? null : new FileInputStream(retFile);
  }

  @Override
  public void cleanup() throws IOException
  {
    for (File file : createdFiles.values()) {
      file.delete();
    }
    createdFiles.clear();
  }
}
