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

package io.druid.segment.data;

import com.google.common.collect.Maps;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
*/
public class TmpFileIOPeon implements IOPeon
{
  Map<String, File> createdFiles = Maps.newLinkedHashMap();

  @Override
  public OutputStream makeOutputStream(String filename) throws IOException
  {
    File retFile = createdFiles.get(filename);
    if (retFile == null) {
      retFile = File.createTempFile("filePeon", filename);
      retFile.deleteOnExit();
      createdFiles.put(filename, retFile);
    }
    return new BufferedOutputStream(new FileOutputStream(retFile));
  }

  @Override
  public InputStream makeInputStream(String filename) throws IOException
  {
    final File retFile = createdFiles.get(filename);

    return retFile == null ? null : new FileInputStream(retFile);
  }

  @Override
  public void cleanup() throws IOException
  {
    for (File file : createdFiles.values()) {
      file.delete();
    }
    createdFiles.clear();
  }
}
