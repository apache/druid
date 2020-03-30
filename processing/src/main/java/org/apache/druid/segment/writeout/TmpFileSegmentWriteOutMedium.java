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

package org.apache.druid.segment.writeout;

import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.io.Closer;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public final class TmpFileSegmentWriteOutMedium implements SegmentWriteOutMedium
{
  private final File dir;
  private final Closer closer = Closer.create();

  TmpFileSegmentWriteOutMedium(File outDir) throws IOException
  {
    File tmpOutputFilesDir = new File(outDir, "tmpOutputFiles");
    org.apache.commons.io.FileUtils.forceMkdir(tmpOutputFilesDir);
    closer.register(() -> FileUtils.deleteDirectory(tmpOutputFilesDir));
    this.dir = tmpOutputFilesDir;
  }

  @Override
  public WriteOutBytes makeWriteOutBytes() throws IOException
  {
    File file = File.createTempFile("filePeon", null, dir);
    FileChannel ch = FileChannel.open(
        file.toPath(),
        StandardOpenOption.READ,
        StandardOpenOption.WRITE
    );
    closer.register(file::delete);
    closer.register(ch);
    return new FileWriteOutBytes(file, ch);
  }

  @Override
  public Closer getCloser()
  {
    return closer;
  }

  @Override
  public void close() throws IOException
  {
    closer.close();
  }
}
