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
import io.druid.segment.store.Directory;
import io.druid.segment.store.IndexInput;
import io.druid.segment.store.IndexInputInputStream;
import io.druid.segment.store.IndexOutput;
import io.druid.segment.store.IndexOutputOutputStream;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * under this Directory,create filePeon prefix temp files.
 * when cleanup was called, these filePeonXX files would be deleted.
 */
public class DirectoryBasedTmpIOPeon implements IOPeon
{
  private final boolean allowOverwrite;

  private Directory directory;
  Map<String, IndexOutput> createdOutputs = Maps.newLinkedHashMap();


  public DirectoryBasedTmpIOPeon(Directory directory)
  {
    this(directory, true);
  }

  public DirectoryBasedTmpIOPeon(Directory directoy, boolean allowOverwrite)
  {
    this.directory = directoy;
    this.allowOverwrite = allowOverwrite;
  }


  @Override
  public OutputStream makeOutputStream(String filename) throws IOException
  {
    IndexOutput indexOutput = createdOutputs.get(filename);
    if (indexOutput == null) {
      IndexOutput tmpIndexOutput = directory.createTempOutput("filePeon", filename);
      createdOutputs.put(filename, tmpIndexOutput);
      return createIndexOutputStream(tmpIndexOutput);
    } else if (allowOverwrite) {
      //overwrite is to delete the origin file and create a new file to be written
      String tmpFileName = indexOutput.getName();
      directory.deleteFile(tmpFileName);
      indexOutput = directory.createOutput(tmpFileName);
      createdOutputs.put(filename, indexOutput);
      return createIndexOutputStream(indexOutput);
    } else {
      throw new IOException("tmp file conflicts, file[" + filename + "] already exist!");
    }


  }

  private OutputStream createIndexOutputStream(IndexOutput indexOutput)
  {
    OutputStream indexOutputOutputStream = new IndexOutputOutputStream(indexOutput);
    return new BufferedOutputStream(indexOutputOutputStream);
  }

  @Override
  public InputStream makeInputStream(String filename) throws IOException
  {
    IndexOutput indexOutput = createdOutputs.get(filename);
    if (indexOutput == null) {
      return null;
    }
    String name = indexOutput.getName();
    IndexInput indexInput = directory.openInput(name);
    return new IndexInputInputStream(indexInput);
  }

  @Override
  public void cleanup() throws IOException
  {
    for (IndexOutput indexOutput : createdOutputs.values()) {
      String name = indexOutput.getName();
      directory.deleteFile(name);
    }
    createdOutputs.clear();
  }

  public boolean isOverwriteAllowed()
  {
    return allowOverwrite;
  }


}
