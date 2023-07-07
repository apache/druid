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

package org.apache.druid.storage.google.output;

import com.google.api.services.storage.model.StorageObject;
import org.apache.druid.storage.remote.ChunkingStorageConnector;
import org.apache.druid.storage.remote.ChunkingStorageConnectorParameters;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

public class GoogleStorageConnector extends ChunkingStorageConnector<StorageObject>
{
  @Override
  public boolean pathExists(String path)
  {
    return false;
  }

  @Override
  public OutputStream write(String path) throws IOException
  {
    return null;
  }

  @Override
  public void deleteFile(String path) throws IOException
  {

  }

  @Override
  public void deleteFiles(Iterable<String> paths) throws IOException
  {

  }

  @Override
  public void deleteRecursively(String path) throws IOException
  {

  }

  @Override
  public Iterator<String> listDir(String dirName) throws IOException
  {
    return null;
  }

  @Override
  public ChunkingStorageConnectorParameters<StorageObject> buildInputParams(String path)
  {
    return null;
  }

  @Override
  public ChunkingStorageConnectorParameters<StorageObject> buildInputParams(String path, long from, long size)
  {
    return null;
  }
}
