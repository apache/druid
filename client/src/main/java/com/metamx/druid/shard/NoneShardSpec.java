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

package com.metamx.druid.shard;

import com.metamx.druid.input.InputRow;
import com.metamx.druid.partition.PartitionChunk;
import com.metamx.druid.partition.SingleElementPartitionChunk;

import java.util.Map;

/**
 */
public class NoneShardSpec implements ShardSpec
{
  @Override
  public <T> PartitionChunk<T> createChunk(T obj)
  {
    return new SingleElementPartitionChunk<T>(obj);
  }

  @Override
  public boolean isInChunk(Map<String, String> dimensions)
  {
    return true;
  }

  @Override
  public boolean isInChunk(InputRow inputRow)
  {
    return true;
  }

  @Override
  public int getPartitionNum()
  {
    return 0;
  }

  @Override
  public boolean equals(Object obj)
  {
    return obj instanceof NoneShardSpec;
  }

  @Override
  public String toString()
  {
    return "NoneShardSpec";
  }
}
