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

package com.metamx.druid.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.metamx.druid.Query;
import org.joda.time.DateTime;

import java.util.Arrays;

public class RequestLogLine
{
  private static final Joiner JOINER = Joiner.on("\t");

  private final DateTime timestamp;
  private final String remoteAddr;
  private final Query query;

  public RequestLogLine(DateTime timestamp, String remoteAddr, Query query)
  {
    this.timestamp = timestamp;
    this.remoteAddr = remoteAddr;
    this.query = query;
  }

  public String getLine(ObjectMapper objectMapper) throws JsonProcessingException
  {
    return JOINER.join(
        Arrays.asList(
            timestamp,
            remoteAddr,
            objectMapper.writeValueAsString(query)
        )
    );
  }
}
