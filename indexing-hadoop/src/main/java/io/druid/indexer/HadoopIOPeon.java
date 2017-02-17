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

package io.druid.indexer;

import io.druid.segment.data.IOPeon;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 */
class HadoopIOPeon implements IOPeon
{
  private final JobContext job;
  private final Path baseDir;
  private final boolean overwriteFiles;

  public HadoopIOPeon(JobContext job, Path baseDir, final boolean overwriteFiles)
  {
    this.job = job;
    this.baseDir = baseDir;
    this.overwriteFiles = overwriteFiles;
  }

  @Override
  public OutputStream makeOutputStream(String filename) throws IOException
  {
    return Utils.makePathAndOutputStream(job, new Path(baseDir, filename), overwriteFiles);
  }

  @Override
  public InputStream makeInputStream(String filename) throws IOException
  {
    return Utils.openInputStream(job, new Path(baseDir, filename));
  }

  @Override
  public void close() throws IOException
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public File getFile(String filename)
  {
    throw new UnsupportedOperationException();
  }
}
