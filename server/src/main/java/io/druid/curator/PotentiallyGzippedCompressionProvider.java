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

package io.druid.curator;

import org.apache.curator.framework.api.CompressionProvider;
import org.apache.curator.framework.imps.GzipCompressionProvider;

import java.io.IOException;

/**
 */
public class PotentiallyGzippedCompressionProvider implements CompressionProvider
{
  private final boolean compressOutput;
  GzipCompressionProvider base = new GzipCompressionProvider();

  public PotentiallyGzippedCompressionProvider(boolean compressOutput)
  {
    this.compressOutput = compressOutput;
  }

  @Override
  public byte[] compress(String path, byte[] data) throws Exception
  {
    return compressOutput ? base.compress(path, data) : data;
  }

  @Override
  public byte[] decompress(String path, byte[] data) throws Exception
  {
    try {
      return base.decompress(path, data);
    }
    catch (IOException e) {
      return data;
    }
  }
}
