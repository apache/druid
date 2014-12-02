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

package io.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.indexer.path.PathSpec;
import io.druid.indexer.updater.MetadataStorageUpdaterJobSpec;
import io.druid.segment.indexing.IOConfig;

import java.util.Map;

/**
 */
@JsonTypeName("hadoop")
public class HadoopIOConfig implements IOConfig
{
  private final PathSpec pathSpec;
  private final MetadataStorageUpdaterJobSpec metadataUpdateSpec;
  private final String segmentOutputPath;

  @JsonCreator
  public HadoopIOConfig(
      final @JsonProperty("inputSpec") PathSpec pathSpec,
      final @JsonProperty("metadataUpdateSpec") MetadataStorageUpdaterJobSpec metadataUpdateSpec,
      final @JsonProperty("segmentOutputPath") String segmentOutputPath
  )
  {
    this.pathSpec = pathSpec;
    this.metadataUpdateSpec = metadataUpdateSpec;
    this.segmentOutputPath = segmentOutputPath;
  }

  @JsonProperty("inputSpec")
  public PathSpec getPathSpec()
  {
    return pathSpec;
  }

  @JsonProperty("metadataUpdateSpec")
  public MetadataStorageUpdaterJobSpec getMetadataUpdateSpec()
  {
    return metadataUpdateSpec;
  }

  @JsonProperty
  public String getSegmentOutputPath()
  {
    return segmentOutputPath;
  }

  public HadoopIOConfig withSegmentOutputPath(String path)
  {
    return new HadoopIOConfig(pathSpec, metadataUpdateSpec, path);
  }
}
