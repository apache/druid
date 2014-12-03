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

package io.druid.segment.realtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.data.input.Firehose;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.IngestionSpec;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.plumber.Plumber;

import java.io.IOException;

/**
 * A Fire Department has a Firehose and a Plumber.
 * 
 * This is a metaphor for a realtime stream (Firehose) and a coordinator of sinks (Plumber). The Firehose provides the
 * realtime stream of data.  The Plumber directs each drop of water from the firehose into the correct sink and makes
 * sure that the sinks don't overflow.
 */
public class FireDepartment extends IngestionSpec<RealtimeIOConfig, RealtimeTuningConfig>
{
  private final DataSchema dataSchema;
  private final RealtimeIOConfig ioConfig;
  private final RealtimeTuningConfig tuningConfig;

  private final FireDepartmentMetrics metrics = new FireDepartmentMetrics();

  @JsonCreator
  public FireDepartment(
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("ioConfig") RealtimeIOConfig ioConfig,
      @JsonProperty("tuningConfig") RealtimeTuningConfig tuningConfig
  )
  {
    super(dataSchema, ioConfig, tuningConfig);
    Preconditions.checkNotNull(dataSchema, "dataSchema");
    Preconditions.checkNotNull(ioConfig, "ioConfig");

    this.dataSchema = dataSchema;
    this.ioConfig = ioConfig;
    this.tuningConfig = tuningConfig == null ? RealtimeTuningConfig.makeDefaultTuningConfig() : tuningConfig;

  }

  /**
   * Provides the data schema for the feed that this FireDepartment is in charge of.
   *
   * @return the Schema for this feed.
   */
  @JsonProperty("dataSchema")
  @Override
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  @JsonProperty("ioConfig")
  @Override
  public RealtimeIOConfig getIOConfig()
  {
    return ioConfig;
  }

  @JsonProperty("tuningConfig")
  @Override
  public RealtimeTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  public Plumber findPlumber()
  {
    return ioConfig.getPlumberSchool().findPlumber(dataSchema, tuningConfig, metrics);
  }

  public Firehose connect() throws IOException
  {
    return ioConfig.getFirehoseFactory().connect(dataSchema.getParser());
  }

  public FireDepartmentMetrics getMetrics()
  {
    return metrics;
  }
}
