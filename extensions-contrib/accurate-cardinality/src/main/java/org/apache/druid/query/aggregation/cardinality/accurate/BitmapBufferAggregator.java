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

package org.apache.druid.query.aggregation.cardinality.accurate;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.Collector;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.CollectorFactory;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;

public class BitmapBufferAggregator implements BufferAggregator
{
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<Collector>> collectors = new IdentityHashMap<>();

  private final BaseObjectColumnValueSelector selector;
  private final CollectorFactory collectorFactory;

  public BitmapBufferAggregator(BaseObjectColumnValueSelector selector, CollectorFactory defaultBitmapFactory)
  {
    this.selector = selector;
    this.collectorFactory = defaultBitmapFactory;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    createNewCollector(buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    Object object = selector.getObject();
    if (object == null) {
      return;
    }
    Collector collector = getOrCreateCollector(buf, position);
    collector.fold((Collector) object);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return getOrCreateCollector(buf, position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("BitmapBufferAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("BitmapBufferAggregator does not support getLong()");
  }

  @Override
  public void close()
  {

  }

  private Collector createNewCollector(ByteBuffer buf, int position)
  {
    buf.position(position);
    Collector collector = collectorFactory.makeEmptyCollector();
    Int2ObjectMap<Collector> collectorMap = collectors.get(buf);
    if (collectorMap == null) {
      collectorMap = new Int2ObjectOpenHashMap<>();
      collectors.put(buf, collectorMap);
    }
    collectorMap.put(position, collector);
    return collector;
  }

  private Collector getOrCreateCollector(ByteBuffer buf, int position)
  {
    Int2ObjectMap<Collector> collectMap = collectors.get(buf);
    Collector collector = collectMap != null ? collectMap.get(position) : null;
    if (collector != null) {
      return collector;
    }
    return createNewCollector(buf, position);
  }
}
