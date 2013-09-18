/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexer.hadoop;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
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

      for (int i = 0; i < testFiles.length; i++) {
        Assert.assertTrue(files.remove(testFiles[i]));
      }

      Assert.assertTrue(files.isEmpty());
    }
    catch (IOException e) {
      Throwables.propagate(e);
    }
    finally {
      try {
        FileUtils.deleteDirectory(baseDir);
      }
      catch (IOException e) {
        Throwables.propagate(e);
      }
    }
  }
}
