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
package com.metamx.druid.master;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DynamicConfigs
{
  public static final String CONFIG_KEY = "master.dynamicConfigs";
  private long millisToWaitBeforeDeleting=15 * 60 * 1000L;
  private long mergeBytesLimit= 100000000L;
  private int mergeSegmentsLimit = Integer.MAX_VALUE;
  private int maxSegmentsToMove = 5;

  @JsonCreator
  public DynamicConfigs(@JsonProperty("millisToWaitBeforeDeleting") Integer millisToWaitBeforeDeleting,
                        @JsonProperty("mergeBytesLimit") Long mergeBytesLimit,
                        @JsonProperty("mergeSegmentsLimit") Integer mergeSegmentsLimit,
                        @JsonProperty("maxSegmentsToMove") Integer maxSegmentsToMove
  )
  {
    if (maxSegmentsToMove!=null)
    {
      this.maxSegmentsToMove=maxSegmentsToMove;
    }
    if (millisToWaitBeforeDeleting!=null)
    {
      this.millisToWaitBeforeDeleting=millisToWaitBeforeDeleting;
    }
    if (mergeSegmentsLimit!=null)
    {
      this.mergeSegmentsLimit=mergeSegmentsLimit;
    }
    if (mergeBytesLimit!=null)
    {
      this.mergeBytesLimit=mergeBytesLimit;
    }
  }

  public DynamicConfigs()
  {
  }

  public static String getConfigKey()
  {
    return CONFIG_KEY;
  }

  public long getMillisToWaitBeforeDeleting()
  {
    return millisToWaitBeforeDeleting;
  }

  public long getMergeBytesLimit()
  {
    return mergeBytesLimit;
  }

  public int getMergeSegmentsLimit()
  {
    return mergeSegmentsLimit;
  }

  public int getMaxSegmentsToMove()
  {
    return maxSegmentsToMove;
  }
}
