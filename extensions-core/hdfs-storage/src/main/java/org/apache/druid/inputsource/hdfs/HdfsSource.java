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

package org.apache.druid.inputsource.hdfs;

import com.google.common.base.Predicate;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.storage.hdfs.HdfsDataSegmentPuller;
import org.apache.druid.utils.CompressionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class HdfsSource implements InputEntity
{
  private final Configuration conf;
  private final Path path;

  HdfsSource(Configuration conf, Path path)
  {
    this.conf = conf;
    this.path = path;
  }

  @Override
  public URI getUri()
  {
    return path.toUri();
  }

  @Override
  public InputStream open() throws IOException
  {
    FileSystem fs = path.getFileSystem(conf);
    return CompressionUtils.decompress(fs.open(path), path.getName());
  }

  @Override
  public Predicate<Throwable> getFetchRetryCondition()
  {
    return HdfsDataSegmentPuller.RETRY_PREDICATE;
  }
}
