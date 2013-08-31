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

package io.druid.indexing.common.tasklogs;

import com.google.common.base.Optional;
import com.google.common.io.InputSupplier;
import com.metamx.common.logger.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class NoopTaskLogs implements TaskLogs
{
  private final Logger log = new Logger(TaskLogs.class);

  @Override
  public Optional<InputSupplier<InputStream>> streamTaskLog(String taskid, long offset) throws IOException
  {
    return Optional.absent();
  }

  @Override
  public void pushTaskLog(String taskid, File logFile) throws IOException
  {
    log.info("Not pushing logs for task: %s", taskid);
  }
}
