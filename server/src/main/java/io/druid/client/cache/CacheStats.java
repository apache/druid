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

package io.druid.client.cache;

/**
 */
public class CacheStats
{
  private final long numHits;
  private final long numMisses;
  private final long size;
  private final long sizeInBytes;
  private final long numEvictions;
  private final long numTimeouts;
  private final long numErrors;

  public CacheStats(
      long numHits,
      long numMisses,
      long size,
      long sizeInBytes,
      long numEvictions,
      long numTimeouts,
      long numErrors
  )
  {
    this.numHits = numHits;
    this.numMisses = numMisses;
    this.size = size;
    this.sizeInBytes = sizeInBytes;
    this.numEvictions = numEvictions;
    this.numTimeouts = numTimeouts;
    this.numErrors = numErrors;
  }

  public long getNumHits()
  {
    return numHits;
  }

  public long getNumMisses()
  {
    return numMisses;
  }

  public long getNumEntries()
  {
    return size;
  }

  public long getSizeInBytes()
  {
    return sizeInBytes;
  }

  public long getNumEvictions()
  {
    return numEvictions;
  }

  public long getNumTimeouts()
  {
    return numTimeouts;
  }

  public long getNumErrors()
  {
    return numErrors;
  }

  public long numLookups()
  {
    return numHits + numMisses;
  }

  public double hitRate()
  {
    long lookups = numLookups();
    return lookups == 0 ? 0 : numHits / (double) lookups;
  }

  public long averageBytes()
  {
    return size == 0 ? 0 : sizeInBytes / size;
  }

  public CacheStats delta(CacheStats oldStats)
  {
    if (oldStats == null) {
      return this;
    }
    return new CacheStats(
        numHits - oldStats.numHits,
        numMisses - oldStats.numMisses,
        size - oldStats.size,
        sizeInBytes - oldStats.sizeInBytes,
        numEvictions - oldStats.numEvictions,
        numTimeouts - oldStats.numTimeouts,
        numErrors - oldStats.numErrors
    );
  }
}
