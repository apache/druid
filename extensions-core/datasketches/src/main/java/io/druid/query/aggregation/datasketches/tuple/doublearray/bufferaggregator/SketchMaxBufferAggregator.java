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
import com.yahoo.sketches.tuple.ArrayOfDoublesCombiner;
import com.yahoo.sketches.tuple.ArrayOfDoublesIntersection;
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
public class SketchMaxBufferAggregator extends SketchIntersectionBufferAggregator{

	public SketchMaxBufferAggregator(ObjectColumnSelector selector, List<FloatColumnSelector> selectors, int size,
			int valuesCount, int maxIntermediateSize) {
		super(selector, selectors, size, valuesCount, maxIntermediateSize);
	}

	@Override
	public ArrayOfDoublesCombiner getCombiner() {
		return SketchAggregator.max;
	}
	
}
