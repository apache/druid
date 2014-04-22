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

package io.druid.indexer.partitions;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.Jobby;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = SingleDimensionPartitionsSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "dimension", value = SingleDimensionPartitionsSpec.class),
    @JsonSubTypes.Type(name = "random", value = RandomPartitionsSpec.class),
    @JsonSubTypes.Type(name = "hashed", value = HashedPartitionsSpec.class)
})
public interface PartitionsSpec
{
  @JsonIgnore
  public Jobby getPartitionJob(HadoopDruidIndexerConfig config);

  @JsonProperty
  public long getTargetPartitionSize();

  @JsonProperty
  public long getMaxPartitionSize();

  @JsonProperty
  public boolean isAssumeGrouped();

  @JsonIgnore
  public boolean isDeterminingPartitions();

  @JsonProperty
  public int getNumShards();

}
