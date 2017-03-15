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

package io.druid.query.aggregation.datasketches.tuple.doublearray.aggregator;

import java.util.List;

import com.metamx.common.IAE;
import com.metamx.common.logger.Logger;
import com.yahoo.memory.Memory;
import com.yahoo.sketches.tuple.ArrayOfDoublesCombiner;
import com.yahoo.sketches.tuple.ArrayOfDoublesIntersection;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUnion;

import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.datasketches.tuple.doublearray.SketchOperations;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.ObjectColumnSelector;

/**
 * 
 * @author sunxin@rongcapital.cn
 *
 */
@SuppressWarnings({"rawtypes"})
public abstract class SketchAggregator implements Aggregator {

	protected static final Logger logger = new Logger(SketchAggregator.class);

    protected final List<FloatColumnSelector> selectors;
    protected final ObjectColumnSelector selector;
    protected final String name;
    protected final int size;
    protected final int valuesCount;


    public SketchAggregator(String name,ObjectColumnSelector selector,List<FloatColumnSelector> selectors,int size,int valuesCount) {
        this.name = name;
        this.selector = selector;
        this.selectors = selectors;
        this.size = size;
        this.valuesCount = valuesCount;
    }

    @Override
    public void aggregate() {
    	Object key = selector.get();
    	double[] values = new double[selectors.size()];
    	for(int i=0;i<values.length;i++){
    		values[i] = selectors.get(i).get();
    	}
    	
        if (key instanceof String  ) {
        	key = SketchOperations.createSketch(this.size,this.valuesCount,key,values);
        }
    	update(key);

    }
    
    public abstract void update(Object key);


    @Override
    public float getFloat() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public long getLong() {
        throw new UnsupportedOperationException("Not implemented");
    }



    public synchronized  static void updateUnion(ArrayOfDoublesUnion union, Object key) {
        if(key == null){
        	return;
        }
    	if(key instanceof ArrayOfDoublesSketch){
        	union.update((ArrayOfDoublesSketch)key);
        }else if(key instanceof Memory){
        	union.update(SketchOperations.deserializeFromMemory((Memory)key));
        }else{
            throw new IAE("Illegal type received while tuple sketch merging [%s]", key.getClass());
        }
    }
    
    public synchronized  static void updateIntersection(ArrayOfDoublesIntersection intersection, Object key,ArrayOfDoublesCombiner combiner) {
        if(key instanceof ArrayOfDoublesSketch){
        	intersection.update((ArrayOfDoublesSketch)key,combiner);
        }else if(key instanceof Memory){
        	intersection.update(SketchOperations.deserializeFromMemory((Memory)key),combiner);
        }else{
            throw new IAE("Illegal type received while tuple sketch merging [%s]", key.getClass());
        }
    }
    
    public static ArrayOfDoublesSketch parseSketch(Object key){
        if(key instanceof ArrayOfDoublesSketch){
        	return (ArrayOfDoublesSketch)key;
        }else if(key instanceof Memory){
        	return SketchOperations.deserializeFromMemory((Memory)key);
        }else{
            throw new IAE("Illegal type received while tuple sketch parsing [%s]", key.getClass());
        }
    }
    
    public static ArrayOfDoublesCombiner max = new ArrayOfDoublesCombiner(){
		@Override
		public double[] combine(double[] a, double[] b) {
			if(a==null || a.length==0){
				return b;
			}
			double[] c = new double[a.length];
			for(int i=0;i<c.length;i++){
				c[i] = Math.max(a[i], b[i]);
			}
			return c;
		}
    };
    
    public static ArrayOfDoublesCombiner min = new ArrayOfDoublesCombiner(){
		@Override
		public double[] combine(double[] a, double[] b) {
			if(a==null || a.length==0){
				return b;
			}
			double[] c = new double[a.length];
			for(int i=0;i<c.length;i++){
				c[i] = Math.min(a[i], b[i]);
			}
			return c;
		}
    };

}
