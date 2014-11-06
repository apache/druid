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

package io.druid.testing.clients;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.indexing.common.TaskStatus;
import org.joda.time.DateTime;

public class TaskResponseObject
{

  private final String id;
  private final DateTime createdTime;
  private final DateTime queueInsertionTime;
  private final TaskStatus status;

  @JsonCreator
  private TaskResponseObject(
      @JsonProperty("id") String id,
      @JsonProperty("createdTime") DateTime createdTime,
      @JsonProperty("queueInsertionTime") DateTime queueInsertionTime,
      @JsonProperty("status") TaskStatus status
  )
  {
    this.id = id;
    this.createdTime = createdTime;
    this.queueInsertionTime = queueInsertionTime;
    this.status = status;
  }

  public String getId()
  {
    return id;
  }

  public DateTime getCreatedTime()
  {
    return createdTime;
  }

  public DateTime getQueueInsertionTime()
  {
    return queueInsertionTime;
  }

  public TaskStatus getStatus()
  {
    return status;
  }
}
