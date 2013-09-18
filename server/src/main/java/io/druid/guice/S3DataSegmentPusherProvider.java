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

package io.druid.guice;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.S3DataSegmentPusher;
import io.druid.segment.loading.S3DataSegmentPusherConfig;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;

import javax.validation.constraints.NotNull;

/**
 */
public class S3DataSegmentPusherProvider implements DataSegmentPusherProvider
{
  @JacksonInject
  @NotNull
  private RestS3Service restS3Service = null;

  @JacksonInject
  @NotNull
  private S3DataSegmentPusherConfig config = null;

  @JacksonInject
  @NotNull
  private ObjectMapper jsonMapper = null;

  @Override
  public DataSegmentPusher get()
  {
    return new S3DataSegmentPusher(restS3Service, config, jsonMapper);
  }
}
