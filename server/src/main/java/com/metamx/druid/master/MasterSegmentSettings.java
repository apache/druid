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

public class MasterSegmentSettings
{
  public static final String CONFIG_KEY = "master.dynamicConfigs";
  private long millisToWaitBeforeDeleting=15 * 60 * 1000L;
  private long mergeBytesLimit= 100000000L;
  private int mergeSegmentsLimit = Integer.MAX_VALUE;
  private int maxSegmentsToMove = 5;
  private boolean emitBalancingStats = false;

  @JsonCreator
  public MasterSegmentSettings(
      @JsonProperty("millisToWaitBeforeDeleting") Long millisToWaitBeforeDeleting,
      @JsonProperty("mergeBytesLimit") Long mergeBytesLimit,
      @JsonProperty("mergeSegmentsLimit") Integer mergeSegmentsLimit,
      @JsonProperty("maxSegmentsToMove") Integer maxSegmentsToMove,
      @JsonProperty("emitBalancingStats") Boolean emitBalancingStats
  )
  {
      this.maxSegmentsToMove=maxSegmentsToMove;
      this.millisToWaitBeforeDeleting=millisToWaitBeforeDeleting;
      this.mergeSegmentsLimit=mergeSegmentsLimit;
      this.mergeBytesLimit=mergeBytesLimit;
      this.emitBalancingStats = emitBalancingStats;
  }

  public static String getConfigKey()
  {
    return CONFIG_KEY;
  }

  @JsonProperty
  public long getMillisToWaitBeforeDeleting()
  {
    return millisToWaitBeforeDeleting;
  }

  @JsonProperty
  public long getMergeBytesLimit()
  {
    return mergeBytesLimit;
  }

  public boolean isEmitBalancingStats()
  {
    return emitBalancingStats;
  }

  @JsonProperty
  public int getMergeSegmentsLimit()
  {
    return mergeSegmentsLimit;
  }

  @JsonProperty
  public int getMaxSegmentsToMove()
  {
    return maxSegmentsToMove;
  }


  public static class Builder
  {
    public static final String CONFIG_KEY = "master.dynamicConfigs";
    private long millisToWaitBeforeDeleting;
    private long mergeBytesLimit;
    private int mergeSegmentsLimit;
    private int maxSegmentsToMove;
    private boolean emitBalancingStats;

    public Builder()
    {
      this.millisToWaitBeforeDeleting=15 * 60 * 1000L;
      this.mergeBytesLimit= 100000000L;
      this.mergeSegmentsLimit= Integer.MAX_VALUE;
      this.maxSegmentsToMove = 5;
      this.emitBalancingStats = false;
    }

    public Builder(long millisToWaitBeforeDeleting, long mergeBytesLimit, int mergeSegmentsLimit, int maxSegmentsToMove, boolean emitBalancingStats)
    {
      this.millisToWaitBeforeDeleting = millisToWaitBeforeDeleting;
      this.mergeBytesLimit = mergeBytesLimit;
      this.mergeSegmentsLimit = mergeSegmentsLimit;
      this.maxSegmentsToMove = maxSegmentsToMove;
      this.emitBalancingStats = emitBalancingStats;
    }

    public Builder withMillisToWaitBeforeDeleting(long millisToWaitBeforeDeleting)
    {
      this.millisToWaitBeforeDeleting=millisToWaitBeforeDeleting;
      return this;
    }

    public Builder withMergeBytesLimit(long mergeBytesLimit)
    {
      this.mergeBytesLimit=mergeBytesLimit;
      return this;
    }

    public Builder withMergeSegmentsLimit(int mergeSegmentsLimit)
    {
      this.mergeSegmentsLimit=mergeSegmentsLimit;
      return this;
    }

    public Builder withMaxSegmentsToMove(int maxSegmentsToMove)
    {
      this.maxSegmentsToMove=maxSegmentsToMove;
      return this;
    }

    public MasterSegmentSettings build()
    {
      return new MasterSegmentSettings(millisToWaitBeforeDeleting,mergeBytesLimit,mergeSegmentsLimit,maxSegmentsToMove, emitBalancingStats);
    }
  }
}
