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

import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

/**
 * An Operator that applies a {@link Processor}, see javadoc on that interface for an explanation.
 */
public class WindowProcessorOperator implements Operator
{
  private final Processor windowProcessor;
  private final Operator child;

  public WindowProcessorOperator(
      Processor windowProcessor,
      Operator child
  )
  {
    this.windowProcessor = windowProcessor;
    this.child = child;
  }

  @Override
  public void open()
  {
    child.open();
  }

  @Override
  public RowsAndColumns next()
  {
    return windowProcessor.process(child.next());
  }

  @Override
  public boolean hasNext()
  {
    return child.hasNext();
  }

  @Override
  public void close(boolean cascade)
  {
    if (cascade) {
      child.close(cascade);
    }
  }
}
