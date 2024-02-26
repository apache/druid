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

package org.apache.druid.sql.calcite.export;

import com.google.common.collect.ImmutableList;
import org.apache.druid.storage.StorageConnector;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

public class TestExportStorageConnector implements StorageConnector
{
  public static final String TYPE_NAME = "testStorage";
  private final ByteArrayOutputStream byteArrayOutputStream;

  public TestExportStorageConnector()
  {
    this.byteArrayOutputStream = new ByteArrayOutputStream();
  }

  public ByteArrayOutputStream getByteArrayOutputStream()
  {
    return byteArrayOutputStream;
  }

  @Override
  public boolean pathExists(String path)
  {
    return true;
  }

  @Override
  public InputStream read(String path)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream readRange(String path, long from, long size)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public OutputStream write(String path)
  {
    return byteArrayOutputStream;
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
    return ImmutableList.<String>of().stream().iterator();
  }
}
