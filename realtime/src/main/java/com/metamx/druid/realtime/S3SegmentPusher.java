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

import com.metamx.druid.loading.S3SegmentPusherConfig;
import com.metamx.druid.loading.SegmentPusher;
import org.codehaus.jackson.map.ObjectMapper;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;

/**
 * A placeholder class to make the move of the SegmentPushers to a new package backwards compatible
 */
@Deprecated
public class S3SegmentPusher extends com.metamx.druid.loading.S3SegmentPusher implements SegmentPusher
{
  public S3SegmentPusher(
      RestS3Service s3Client,
      S3SegmentPusherConfig config,
      ObjectMapper jsonMapper
  )
  {
    super(s3Client, config, jsonMapper);
  }
}
