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

package com.metamx.druid.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.IOException;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
                  @JsonSubTypes.Type(name = "kafka-0.7.2", value = KafkaFirehoseFactory.class),
                  @JsonSubTypes.Type(name = "clipped", value = ClippedFirehoseFactory.class),
                  @JsonSubTypes.Type(name = "timed", value = TimedShutoffFirehoseFactory.class)
              })
public interface FirehoseFactory
{
  /**
   * Initialization method that connects up the fire hose.  If this method returns successfully it should be safe to
   * call hasMore() on the returned Firehose (which might subsequently block).
   * <p/>
   * If this method returns null, then any attempt to call hasMore(), nextRow(), commit() and close() on the return
   * value will throw a surprising NPE.   Throwing IOException on connection failure or runtime exception on
   * invalid configuration is preferred over returning null.
   */
  public Firehose connect() throws IOException;
}
