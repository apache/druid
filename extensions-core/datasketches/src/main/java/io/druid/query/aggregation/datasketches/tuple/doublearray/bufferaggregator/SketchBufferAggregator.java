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
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.MemoryRegion;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Union;
import com.yahoo.sketches.tuple.ArrayOfDoublesSetOperationBuilder;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUnion;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.datasketches.tuple.doublearray.SketchOperations;
import io.druid.query.aggregation.datasketches.tuple.doublearray.aggregator.SketchAggregator;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.ObjectColumnSelector;

/**
 * 
 * @author sunxin@rongcapital.cn
 *
 */
@SuppressWarnings({ "rawtypes", "unused" })
public abstract class SketchBufferAggregator implements BufferAggregator {
	protected static final Logger logger = new Logger(SketchAggregator.class);

	protected final ObjectColumnSelector selector;
	protected final List<FloatColumnSelector> selectors;
	protected final int size;
	protected final int valuesCount;
	protected final int maxIntermediateSize;

	protected NativeMemory nm;

	public SketchBufferAggregator(ObjectColumnSelector selector, List<FloatColumnSelector> selectors, int size,
			int valuesCount,int maxIntermediateSize) {
		this.selector = selector;
		this.selectors = selectors;
		this.size = size;
		this.valuesCount = valuesCount;
		this.maxIntermediateSize = maxIntermediateSize;
	}

	@Override
	public void init(ByteBuffer buf, int position) {
		if (nm == null) {
			nm = new NativeMemory(buf);
		}
	    Memory mem = new MemoryRegion(nm, position, maxIntermediateSize);
	    register(position, mem);
	}
	
	public abstract void register(int position,Memory mem);

	@Override
	public void aggregate(ByteBuffer buf, int position) {
    	Object key = selector.get();
    	double[] values = new double[selectors.size()];
    	for(int i=0;i<values.length;i++){
    		values[i] = selectors.get(i).get();
    	}
    	
		
        if (key instanceof String && values instanceof double[] ) {
        	key = SketchOperations.createSketch(this.size,this.valuesCount,key,values);
        }
        update(buf,position,key);
	}
	
    public abstract void update(ByteBuffer buf,int position,Object key);


	@Override
	public float getFloat(ByteBuffer buf, int position) {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public long getLong(ByteBuffer buf, int position) {
		throw new UnsupportedOperationException("Not implemented");
	}

}
