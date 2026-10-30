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

package org.apache.druid.msq.querykit.scan;

/**
 * Decides which rows of one cursor a {@link ScanQueryFrameProcessor} writes, as directed by a
 * {@link ScanLimitTracker}. {@link #check()} and {@link #onRowWritten()} act on the cursor's current row, so they must
 * be called before the cursor advances. Each gate serves a single cursor on a single thread.
 */
public interface RowGate
{
  /**
   * Gate that writes every row and tracks nothing.
   */
  RowGate ALWAYS_WRITE = new RowGate()
  {
    @Override
    public Decision check()
    {
      return Decision.WRITE;
    }

    @Override
    public void onRowWritten()
    {
    }

    @Override
    public void onFrameFlushed()
    {
    }
  };

  enum Decision
  {
    /**
     * Write the current row to output.
     */
    WRITE,

    /**
     * Skip writing current row to output, but continue scanning the input.
     */
    SKIP,

    /**
     * Stop scanning the input and stop writing to output.
     */
    STOP
  }

  /**
   * Returns the decision for the cursor's current row.
   */
  Decision check();

  /**
   * Called after the current row, which {@link #check()} allowed, is added to a frame.
   */
  void onRowWritten();

  /**
   * Called after a frame of {@code numRows} rows from this cursor is written to the output channel.
   */
  void onFrameFlushed();
}
