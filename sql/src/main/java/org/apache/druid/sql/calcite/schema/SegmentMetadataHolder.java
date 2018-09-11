/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.druid.sql.calcite.schema;

import org.apache.druid.sql.calcite.table.RowSignature;

import javax.annotation.Nullable;

public class SegmentMetadataHolder
{
  private final Object lock = new Object();
  private RowSignature rowSignature;
  private final long isPublished;
  private final long isAvailable;
  private final long isRealtime;
  private long numReplicas;
  @Nullable
  private Long numRows;


  public SegmentMetadataHolder(
      @Nullable RowSignature rowSignature,
      long isPublished,
      long isAvailable,
      long isRealtime,
      long numReplicas,
      @Nullable Long numRows
  )
  {
    this.rowSignature = rowSignature;
    this.isPublished = isPublished;
    this.isAvailable = isAvailable;
    this.isRealtime = isRealtime;
    this.numReplicas = numReplicas;
    this.numRows = numRows;
  }


  public long isPublished()
  {
    synchronized (lock) {
      return isPublished;
    }
  }

  public long isAvailable()
  {
    synchronized (lock) {
      return isAvailable;
    }
  }

  public long isRealtime()
  {
    synchronized (lock) {
      return isRealtime;
    }
  }

  public long getNumReplicas()
  {
    synchronized (lock) {
      return numReplicas;
    }
  }

  @Nullable
  public Long getNumRows()
  {
    synchronized (lock) {
      return numRows;
    }
  }

  @Nullable
  public RowSignature getRowSignature()
  {
    synchronized (lock) {
      return rowSignature;
    }
  }

  public void setRowSignature(RowSignature rowSignature)
  {
    synchronized (lock) {
      this.rowSignature = rowSignature;
      lock.notifyAll();
    }
  }

  public void setNumRows(Long rows)
  {
    synchronized (lock) {
      this.numRows = rows;
      lock.notifyAll();
    }
  }


  public void setNumReplicas(long replicas)
  {
    synchronized (lock) {
      this.numReplicas = replicas;
      lock.notifyAll();
    }
  }

}
