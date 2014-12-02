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
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.IngestionSpec;

/**
 */
public class HadoopIngestionSpec extends IngestionSpec<HadoopIOConfig, HadoopTuningConfig>
{
  private final DataSchema dataSchema;
  private final HadoopIOConfig ioConfig;
  private final HadoopTuningConfig tuningConfig;

  @JsonCreator
  public HadoopIngestionSpec(
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("ioConfig") HadoopIOConfig ioConfig,
      @JsonProperty("tuningConfig") HadoopTuningConfig tuningConfig
  )
  {
    super(dataSchema, ioConfig, tuningConfig);

    this.dataSchema = dataSchema;
    this.ioConfig = ioConfig;
    this.tuningConfig = tuningConfig == null ? HadoopTuningConfig.makeDefaultTuningConfig() : tuningConfig;
  }

  @JsonProperty("dataSchema")
  @Override
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  @JsonProperty("ioConfig")
  @Override
  public HadoopIOConfig getIOConfig()
  {
    return ioConfig;
  }

  @JsonProperty("tuningConfig")
  @Override
  public HadoopTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  public HadoopIngestionSpec withDataSchema(DataSchema schema)
  {
    return new HadoopIngestionSpec(
        schema,
        ioConfig,
        tuningConfig
    );
  }

  public HadoopIngestionSpec withIOConfig(HadoopIOConfig config)
  {
    return new HadoopIngestionSpec(
        dataSchema,
        config,
        tuningConfig
    );
  }

  public HadoopIngestionSpec withTuningConfig(HadoopTuningConfig config)
  {
    return new HadoopIngestionSpec(
        dataSchema,
        ioConfig,
        config
    );
  }
}
