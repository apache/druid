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

package org.apache.druid.indexer.hadoop;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 */
public class FSSpideringIteratorTest
{
  @Test
  public void testIterator()
  {
    String[] testFiles = {"file1", "file2", "file3", "file4", "file5"};

    File baseDir = Files.createTempDir();

    try {
      new File(baseDir, "dir1").mkdir();
      new File(baseDir, "dir1/file1").createNewFile();
      new File(baseDir, "dir1/file2").createNewFile();

      new File(baseDir, "dir2/subDir1").mkdirs();
      new File(baseDir, "dir2/subDir1/file3").createNewFile();
      new File(baseDir, "dir2/subDir2").mkdirs();
      new File(baseDir, "dir2/subDir2/file4").createNewFile();
      new File(baseDir, "dir2/subDir2/file5").createNewFile();

      List<String> files = Lists.newArrayList(
          Iterables.transform(
              FSSpideringIterator.spiderIterable(
                  FileSystem.getLocal(new Configuration()),
                  new Path(baseDir.toString())
              ),
              new Function<FileStatus, String>()
              {
                @Override
                public String apply(@Nullable FileStatus input)
                {
                  return input.getPath().getName();
                }
              }
          )
      );

      for (String testFile : testFiles) {
        Assert.assertTrue(files.remove(testFile));
      }

      Assert.assertTrue(files.isEmpty());
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    finally {
      try {
        FileUtils.deleteDirectory(baseDir);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
