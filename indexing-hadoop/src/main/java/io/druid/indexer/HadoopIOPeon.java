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

package io.druid.indexer;

import io.druid.segment.data.IOPeon;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;

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
  public void cleanup() throws IOException
  {
    throw new UnsupportedOperationException();
  }
}
