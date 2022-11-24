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

package org.apache.druid.query.operator;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.segment.Segment;

public class SegmentToRowsAndColumnsOperator implements Operator
{
  private final Segment segment;
  private boolean hasNext = true;

  public SegmentToRowsAndColumnsOperator(
      Segment segment
  )
  {
    this.segment = segment;
  }

  @Override
  public void open()
  {

  }

  @Override
  public RowsAndColumns next()
  {
    hasNext = false;

    RowsAndColumns rac = segment.as(RowsAndColumns.class);
    if (rac != null) {
      return rac;
    }

    throw new ISE("Cannot work with segment of type[%s]", segment.getClass());
  }

  @Override
  public boolean hasNext()
  {
    return hasNext;
  }

  @Override
  public void close(boolean cascade)
  {

  }
}
