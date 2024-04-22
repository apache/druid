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

package org.apache.druid.sql.calcite;

import org.apache.druid.java.util.common.FileUtils;

import java.io.File;
import java.io.IOException;

public class TempDirProducer
{
  private static TempDirProducer INSTANCE = null;

  public static TempDirProducer instance()
  {
    if (INSTANCE == null) {
      INSTANCE = new TempDirProducer();
    }
    return INSTANCE;
  }

  private final File tempDir;

  private TempDirProducer()
  {
    tempDir = FileUtils.createTempDir("druid-tempdir");
    Runtime.getRuntime().addShutdownHook(new Thread()
    {
      @Override
      public void run()
      {
        try {
          FileUtils.deleteDirectory(tempDir);
        }
        catch (IOException ex) {
          ex.printStackTrace();
        }
      }
    });
  }

  public TempDirProducer(TempDirProducer tempDirProducer, String prefix)
  {
    this.tempDir = FileUtils.createTempDirInLocation(tempDirProducer.tempDir.toPath(), prefix);
  }

  public TempDirProducer getProducer(String prefix)
  {
    return new TempDirProducer(this, prefix);
  }

  public File getTempDir(String prefix)
  {
    return FileUtils.createTempDirInLocation(tempDir.toPath(), prefix);
  }
}
