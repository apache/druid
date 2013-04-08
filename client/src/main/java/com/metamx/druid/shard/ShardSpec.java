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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.partition.PartitionChunk;

import java.util.Map;

/**
 * A Marker interface that exists to combine ShardSpec objects together for Jackson
 */
@JsonTypeInfo(use= JsonTypeInfo.Id.NAME, property="type", include=JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
    @JsonSubTypes.Type(name="single", value=SingleDimensionShardSpec.class),
    @JsonSubTypes.Type(name="none", value=NoneShardSpec.class),
    @JsonSubTypes.Type(name="linear", value=LinearShardSpec.class)
})
public interface ShardSpec
{
  public <T> PartitionChunk<T> createChunk(T obj);
  public boolean isInChunk(Map<String, String> dimensions);
  public boolean isInChunk(InputRow inputRow);
  public int getPartitionNum();
}
