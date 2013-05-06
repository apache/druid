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

package com.metamx.druid.realtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.druid.realtime.firehose.Firehose;
import com.metamx.druid.realtime.firehose.FirehoseFactory;
import com.metamx.druid.realtime.plumber.Plumber;
import com.metamx.druid.realtime.plumber.PlumberSchool;

import java.io.IOException;

/**
 * A Fire Department has a Firehose and a Plumber.
 *
 * This is a metaphor for a realtime stream (Firehose) and a master of sinks (Plumber). The Firehose provides the
 * realtime stream of data.  The Plumber directs each drop of water from the firehose into the correct sink and makes
 * sure that the sinks don't overflow.
 */
public class FireDepartment
{
  @JsonProperty("schema")
  private final Schema schema;

  @JsonProperty
  private final FireDepartmentConfig config;

  @JsonProperty
  private final FirehoseFactory firehoseFactory;

  @JsonProperty
  private final PlumberSchool plumberSchool;

  private final FireDepartmentMetrics metrics = new FireDepartmentMetrics();

  @JsonCreator
  public FireDepartment(
    @JsonProperty("schema") Schema schema,
    @JsonProperty("config") FireDepartmentConfig config,
    @JsonProperty("firehose") FirehoseFactory firehoseFactory,
    @JsonProperty("plumber") PlumberSchool plumberSchool
  )
  {
    this.schema = schema;
    this.config = config;
    this.firehoseFactory = firehoseFactory;
    this.plumberSchool = plumberSchool;
  }

  /**
   * Provides the data schema for the feed that this FireDepartment is in charge of.
   *
   * @return the Schema for this feed.
   */
  public Schema getSchema()
  {
    return schema;
  }

  public FireDepartmentConfig getConfig()
  {
    return config;
  }

  public Plumber findPlumber()
  {
    return plumberSchool.findPlumber(schema, metrics);
  }

  public Firehose connect() throws IOException
  {
    return firehoseFactory.connect();
  }

  public FireDepartmentMetrics getMetrics()
  {
    return metrics;
  }
}
