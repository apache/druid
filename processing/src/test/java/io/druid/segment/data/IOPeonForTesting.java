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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
*/
class IOPeonForTesting implements IOPeon
{
  Map<String, ByteArrayOutputStream> outStreams = Maps.newHashMap();

  @Override
  public OutputStream makeOutputStream(String filename) throws IOException
  {
    ByteArrayOutputStream stream = outStreams.get(filename);

    if (stream == null) {
      stream = new ByteArrayOutputStream();
      outStreams.put(filename, stream);
    }

    return stream;
  }

  @Override
  public InputStream makeInputStream(String filename) throws IOException
  {
    ByteArrayOutputStream outStream = outStreams.get(filename);

    if (outStream == null) {
      throw new FileNotFoundException(String.format("unknown file[%s]", filename));
    }

    return new ByteArrayInputStream(outStream.toByteArray());
  }

  @Override
  public void cleanup() throws IOException
  {
    outStreams.clear();
  }
}
