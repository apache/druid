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

package org.apache.druid.query;

import org.apache.druid.frame.Frame;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.column.RowSignature;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents an inline datasource where the rows are embedded within the DataSource object itself.
 * <p>
 * The rows are backed by a sequence of {@link FrameSignaturePair}, which contain the Frame representation of the rows
 * represented by the datasource.
 * <p>
 * Note that the signature of the datasource can be different from the signatures of the constituent frames that it
 * consists of. While fetching the iterables, it is the job of this class to make sure that the rows correspond to the
 * {@link #rowSignature}. For frames that donot contain the columns present in the {@link #rowSignature}, they are
 * populated with {@code null}.
 */
public class FrameBasedInlineDataSource implements DataSource
{

  final List<FrameSignaturePair> frames;
  final RowSignature rowSignature;

  public FrameBasedInlineDataSource(
      List<FrameSignaturePair> frames,
      RowSignature rowSignature
  )
  {
    this.frames = frames;
    this.rowSignature = rowSignature;
  }

  public List<FrameSignaturePair> getFrames()
  {
    return frames;
  }

  public RowSignature getRowSignature()
  {
    return rowSignature;
  }

  public Sequence<Object[]> getRowsAsSequence()
  {
    final Sequence<Cursor> cursorSequence =
        Sequences.simple(frames)
                 .flatMap(
                     frameSignaturePair -> {
                       Frame frame = frameSignaturePair.getFrame();
                       RowSignature frameSignature = frameSignaturePair.getRowSignature();
                       FrameReader frameReader = FrameReader.create(frameSignature);
                       final CursorHolder holder = frameReader.makeCursorFactory(frame).makeCursorHolder(
                           CursorBuildSpec.FULL_SCAN
                       );
                       return Sequences.simple(Collections.singletonList(holder.asCursor())).withBaggage(holder);
                     }
                 );

    return cursorSequence.flatMap(
        (cursor) -> {
          final ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
          final List<BaseObjectColumnValueSelector> selectors = rowSignature
              .getColumnNames()
              .stream()
              .map(columnSelectorFactory::makeColumnValueSelector)
              .collect(Collectors.toList());

          return Sequences.simple(
              () -> new Iterator<>()
              {
                @Override
                public boolean hasNext()
                {
                  return !cursor.isDone();
                }

                @Override
                public Object[] next()
                {

                  Object[] row = new Object[rowSignature.size()];
                  for (int i = 0; i < rowSignature.size(); ++i) {
                    row[i] = selectors.get(i).getObject();
                  }

                  cursor.advance();

                  return row;
                }
              }
          );
        }
    );
  }

  @Override
  public Set<String> getTableNames()
  {
    return Collections.emptySet();
  }

  @Override
  public List<DataSource> getChildren()
  {
    return Collections.emptyList();
  }

  @Override
  public DataSource withChildren(List<DataSource> children)
  {
    if (!children.isEmpty()) {
      throw new IAE("Cannot accept children");
    }

    return this;
  }

  @Override
  public boolean isCacheable(boolean isBroker)
  {
    return false;
  }

  @Override
  public boolean isGlobal()
  {
    return true;
  }

  @Override
  public boolean isConcrete()
  {
    return true;
  }

  @Override
  public Function<SegmentReference, SegmentReference> createSegmentMapFunction(Query query, AtomicLong cpuTimeAcc)
  {
    return Function.identity();
  }

  @Override
  public DataSource withUpdatedDataSource(DataSource newSource)
  {
    return newSource;
  }

  @Override
  public byte[] getCacheKey()
  {
    return null;
  }

  @Override
  public DataSourceAnalysis getAnalysis()
  {
    return new DataSourceAnalysis(this, null, null, Collections.emptyList());
  }
}
