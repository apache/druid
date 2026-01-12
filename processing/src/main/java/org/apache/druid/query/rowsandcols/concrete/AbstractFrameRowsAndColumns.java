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
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.semantic.WireTransferable;
import org.apache.druid.frame.wire.FrameWireTransferable;
import org.apache.druid.segment.CloseableShapeshifter;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Objects;

public abstract class AbstractFrameRowsAndColumns implements FrameRowsAndColumns, AutoCloseable, CloseableShapeshifter
{
  final Frame frame;
  @Nullable
  final RowSignature signature;
  final LinkedHashMap<String, Column> colCache = new LinkedHashMap<>();

  public AbstractFrameRowsAndColumns(Frame frame, @Nullable RowSignature signature)
  {
    this.frame = frame;
    this.signature = signature;
  }

  @Override
  public Frame getFrame()
  {
    return frame;
  }

  public boolean hasSignature()
  {
    return signature != null;
  }

  @Override
  public RowSignature getSignature()
  {
    if (signature == null) {
      throw DruidException.defensive("No signature present, cannot call getSignature()");
    }
    return signature;
  }

  @Override
  public Collection<String> getColumnNames()
  {
    if (signature == null) {
      throw DruidException.defensive("No signature present, cannot call getColumnNames()");
    }
    return signature.getColumnNames();
  }

  @Override
  public int numRows()
  {
    return frame.numRows();
  }

  @SuppressWarnings("unchecked")
  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    if (CursorFactory.class.equals(clazz) && signature != null) {
      return (T) FrameReader.create(signature).makeCursorFactory(frame);
    }
    if (WireTransferable.class.equals(clazz)) {
      return (T) new FrameWireTransferable(frame, signature);
    }
    if (Frame.class.equals(clazz)) {
      return (T) frame;
    }
    if (clazz.isInstance(this)) {
      return (T) this;
    }
    return null;
  }

  @Override
  public void close()
  {
    // nothing to close
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AbstractFrameRowsAndColumns that = (AbstractFrameRowsAndColumns) o;
    return Objects.equals(frame, that.frame) && Objects.equals(signature, that.signature);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(frame, signature);
  }
}
