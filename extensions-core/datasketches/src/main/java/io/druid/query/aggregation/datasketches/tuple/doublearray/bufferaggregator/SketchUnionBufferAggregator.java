/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.datasketches.tuple.doublearray.bufferaggregator;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryRegion;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Union;
import com.yahoo.sketches.tuple.ArrayOfDoublesSetOperationBuilder;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUnion;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.datasketches.tuple.doublearray.aggregator.SketchAggregator;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.ObjectColumnSelector;

/**
 * 
 * @author sunxin@rongcapital.cn
 *
 */
@SuppressWarnings({ "rawtypes", "unused" })
public class SketchUnionBufferAggregator extends SketchBufferAggregator {

	protected final Map<Integer, ArrayOfDoublesUnion> unions = new HashMap<>(); 

	public SketchUnionBufferAggregator(ObjectColumnSelector selector, List<FloatColumnSelector> selectors, int size,
			int valuesCount,int maxIntermediateSize) {
		super(selector, selectors, size, valuesCount, maxIntermediateSize);
	}

	@Override
	public void register(int position,Memory mem) {
		unions.put(position, new ArrayOfDoublesSetOperationBuilder().setMemory(mem).setNominalEntries(size).setNumberOfValues(valuesCount).buildUnion());
		
	}

	@Override
	public void update(ByteBuffer buf,int position,Object key) {
    	SketchAggregator.updateUnion(getUnion(buf,position),key);
	}

	

	@Override
	public Object get(ByteBuffer buf, int position) {
		return getUnion(buf, position).getResult();
	}


	@Override
	public void close() {
		unions.clear();
	}

	// Note that this is not threadsafe and I don't think it needs to be
	private ArrayOfDoublesUnion getUnion(ByteBuffer buf, int position) {
		ArrayOfDoublesUnion union = unions.get(position);
		if (union == null) {
		    Memory mem = new MemoryRegion(nm, position, maxIntermediateSize);
		    union = new ArrayOfDoublesSetOperationBuilder().setMemory(mem).setNominalEntries(this.size).setNumberOfValues(this.valuesCount).buildUnion();
			unions.put(position, union);
		}
		return union;
	}
	
}
