/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.indexing.common.config;

import com.google.common.base.Joiner;
import org.skife.config.Config;
import org.skife.config.Default;

import java.io.File;

public abstract class TaskConfig
{
  private static Joiner joiner = Joiner.on("/");

  @Config("druid.indexer.baseDir")
  @Default("/tmp/")
  public abstract String getBaseDir();

  @Config("druid.indexer.taskDir")
  public File getBaseTaskDir()
  {
    return new File(defaultPath("persistent/task"));
  }

  @Config("druid.indexer.hadoopWorkingPath")
  public String getHadoopWorkingPath()
  {
    return defaultPath("druid-indexing");
  }

  @Config("druid.indexer.rowFlushBoundary")
  @Default("500000")
  public abstract int getDefaultRowFlushBoundary();

  private String defaultPath(String subPath)
  {
    return joiner.join(getBaseDir(), subPath);
  }
}