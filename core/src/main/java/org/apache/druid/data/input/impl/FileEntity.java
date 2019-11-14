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

package org.apache.druid.data.input.impl;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.utils.CompressionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class FileEntity implements InputEntity<File>
{
  private final File file;

  FileEntity(File file)
  {
    this.file = file;
  }

  @Override
  public CleanableFile fetch(File temporaryDirectory, byte[] fetchBuffer)
  {
    return new CleanableFile()
    {
      @Override
      public File file()
      {
        return file;
      }

      @Override
      public void close()
      {
        // do nothing
      }
    };
  }

  @Override
  public File getObject()
  {
    return file;
  }

  @Override
  public InputStream open() throws IOException
  {
    return CompressionUtils.decompress(new FileInputStream(file), file.getName());
  }

  @Override
  public Predicate<Throwable> getFetchRetryCondition()
  {
    return Predicates.alwaysFalse();
  }
}
