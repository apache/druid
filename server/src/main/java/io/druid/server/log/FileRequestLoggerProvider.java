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

package io.druid.server.log;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import io.druid.guice.annotations.Json;

import javax.validation.constraints.NotNull;
import java.io.File;

/**
 */
@JsonTypeName("file")
public class FileRequestLoggerProvider implements RequestLoggerProvider
{
  @JsonProperty
  @NotNull
  private File dir = null;

  @JacksonInject
  @NotNull
  private ScheduledExecutorFactory factory = null;


  @JacksonInject
  @NotNull
  @Json
  private ObjectMapper jsonMapper = null;

  @Override
  public RequestLogger get()
  {
    return new FileRequestLogger(jsonMapper, factory.create(1, "RequestLogger-%s"), dir);
  }
}
