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

package org.apache.druid.query.rowsandcols.concrete;

import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.column.RowSignature;

/**
 * Wrapper around {@link Frame} with an optional signature (see {@link #getSignature()}).
 *
 * If the signature is available, this object has working {@link #getColumnNames()} and {@link #findColumn(String)}
 * methods, and can become a {@link CursorFactory} through {@link #as(Class)}. If the signature is not available,
 * this object can only really be used by getting the underlying {@link Frame} via {@link #getFrame()} or
 * {@link #as(Class)}.
 */
public interface FrameRowsAndColumns extends RowsAndColumns
{
  /**
   * Returns the frame.
   */
  Frame getFrame();

  /**
   * Returns a signature, which may or may not be available.
   *
   * @throws DruidException if signature is not available
   */
  RowSignature getSignature();
}
