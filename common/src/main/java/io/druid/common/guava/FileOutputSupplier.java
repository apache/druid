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

package io.druid.common.guava;

import com.google.common.io.OutputSupplier;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
*/
public class FileOutputSupplier implements OutputSupplier<OutputStream>
{
  private final File file;
  private final boolean append;

  public FileOutputSupplier(File file, boolean append)
  {
    this.file = file;
    this.append = append;
  }

  @Override
  public OutputStream getOutput() throws IOException
  {
    return new FileOutputStream(file, append);
  }

  public File getFile()
  {
    return file;
  }
}
