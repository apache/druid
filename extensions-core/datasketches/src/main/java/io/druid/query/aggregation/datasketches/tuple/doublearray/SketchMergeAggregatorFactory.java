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

package io.druid.query.aggregation.datasketches.tuple.doublearray;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.IAE;
import com.yahoo.sketches.tuple.ArrayOfDoublesIntersection;
import com.yahoo.sketches.tuple.ArrayOfDoublesSetOperationBuilder;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketchIterator;
import com.yahoo.sketches.tuple.ArrayOfDoublesUnion;

import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.datasketches.tuple.doublearray.aggregator.EmptySketchAggregator;
import io.druid.query.aggregation.datasketches.tuple.doublearray.aggregator.SketchAggregator;
import io.druid.query.aggregation.datasketches.tuple.doublearray.aggregator.SketchMaxAggregator;
import io.druid.query.aggregation.datasketches.tuple.doublearray.aggregator.SketchMinAggregator;
import io.druid.query.aggregation.datasketches.tuple.doublearray.aggregator.SketchUnionAggregator;
import io.druid.query.aggregation.datasketches.tuple.doublearray.bufferaggregator.EmptySketchBufferAggregator;
import io.druid.query.aggregation.datasketches.tuple.doublearray.bufferaggregator.SketchMaxBufferAggregator;
import io.druid.query.aggregation.datasketches.tuple.doublearray.bufferaggregator.SketchMinBufferAggregator;
import io.druid.query.aggregation.datasketches.tuple.doublearray.bufferaggregator.SketchUnionBufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.ObjectColumnSelector;

/**
 * 
 * @author sunxin@rongcapital.cn
 *
 */
public class SketchMergeAggregatorFactory extends SketchAggregatorFactory {
    private static final byte CACHE_TYPE_ID = 15;

    private final boolean shouldFinalize;
    private final boolean isInputTupleSketch;
    private final int valuesCount;
    private final String valuesFunction;
    
    private final String FUNC_SUM = "sum";
    private final String FUNC_MAX = "max";
    private final String FUNC_MIN = "min";

    @JsonCreator
    public SketchMergeAggregatorFactory(
            @JsonProperty("name") String name,
            @JsonProperty("fieldName") String fieldName,
            @JsonProperty("fieldNames") List<String> fieldNames,
            @JsonProperty("size") Integer size,
            @JsonProperty("shouldFinalize") Boolean shouldFinalize,
            @JsonProperty("isInputTupleSketch") Boolean isInputTupleSketch,
            @JsonProperty("valuesCount") int valuesCount,
            @JsonProperty("valuesFunction") String valuesFunction){
        super(name,fieldName, fieldNames, size, CACHE_TYPE_ID);
        this.shouldFinalize = (shouldFinalize == null) ? true : shouldFinalize.booleanValue();
        this.isInputTupleSketch = (isInputTupleSketch == null) ? false : isInputTupleSketch.booleanValue();
        this.valuesCount=valuesCount;
        this.valuesFunction = valuesFunction;
     }
    
    @SuppressWarnings({"rawtypes" })
    @Override
    public Aggregator factorize(ColumnSelectorFactory metricFactory) {
        ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
        List<FloatColumnSelector> selectors = new ArrayList<>();
        for(String fieldName : fieldNames){
            FloatColumnSelector s = metricFactory.makeFloatColumnSelector(fieldName);
            selectors.add(s);
        }

        if (selector==null || selectors.isEmpty()) {
            return new EmptySketchAggregator(name);
        }
        
        switch(this.valuesFunction){
        case FUNC_SUM:
            return new SketchUnionAggregator(name, selector,selectors, size,valuesCount);
        case FUNC_MAX:
            return new SketchMaxAggregator(name, selector, selectors, size, valuesCount);
        case FUNC_MIN:
            return new SketchMinAggregator(name, selector, selectors, size, valuesCount);
        default:
            throw new IAE("not support this function "+this.valuesFunction);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory) {
        ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
        List<FloatColumnSelector> selectors = new ArrayList<>();
        for(String fieldName : fieldNames){
            FloatColumnSelector s = metricFactory.makeFloatColumnSelector(fieldName);
            selectors.add(s);
        }
        if (selector==null || selectors.isEmpty()) {
            return new EmptySketchBufferAggregator();
        }
        switch(this.valuesFunction){
        case FUNC_SUM:
            return new SketchUnionBufferAggregator(selector,selectors, size, valuesCount,getMaxIntermediateSize());
        case FUNC_MAX:
            return new SketchMaxBufferAggregator(selector,selectors, size, valuesCount,getMaxIntermediateSize());
        case FUNC_MIN:
            return new SketchMinBufferAggregator(selector,selectors, size, valuesCount,getMaxIntermediateSize());
        default:
            throw new IAE("not support this function "+this.valuesFunction);
        }
    }


    @Override
    public int getMaxIntermediateSize() {
        return ArrayOfDoublesUnion.getMaxBytes(size, valuesCount);
    }
    
    @Override
    public List<AggregatorFactory> getRequiredColumns() {
        return Collections.<AggregatorFactory>singletonList(
                new SketchMergeAggregatorFactory(
                        name,
                        fieldName,
                        fieldNames,
                        size,
                        shouldFinalize,
                        isInputTupleSketch,
                        valuesCount,
                        valuesFunction
                )
        );
    }


    @Override
    public AggregatorFactory getCombiningFactory() {
        return new SketchMergeAggregatorFactory(name, fieldName,fieldNames, size, shouldFinalize, false,valuesCount,valuesFunction);
    }

    @Override
    public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException {
        if (other.getName().equals(this.getName()) && other instanceof SketchMergeAggregatorFactory) {
            SketchMergeAggregatorFactory castedOther = (SketchMergeAggregatorFactory) other;

            return new SketchMergeAggregatorFactory(
                    name,
                    fieldName,
                    fieldNames,
                    Math.max(size, castedOther.size),
                    shouldFinalize,
                    true,valuesCount,valuesFunction
            );
        } else {
            throw new AggregatorFactoryNotMergeableException(this, other);
        }
    }

    @JsonProperty
    public boolean getShouldFinalize() {
        return shouldFinalize;
    }

    @JsonProperty
    public boolean getIsInputTupleSketch() {
        return isInputTupleSketch;
    }

    @JsonProperty
    public int getValuesCount(){
        return valuesCount;
    }
    
    @JsonProperty
    public String getValuesFunction(){
        return valuesFunction;
    }
    
    @Override
    public Object combine(Object lhs, Object rhs) {
        switch(this.valuesFunction){
        case FUNC_SUM:{
            ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().setNominalEntries(size).setNumberOfValues(this.valuesCount).buildUnion();
            SketchAggregator.updateUnion(union, lhs);
            SketchAggregator.updateUnion(union, rhs);
            return union.getResult();
        }
        case FUNC_MAX:{
            ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().setNominalEntries(size).setNumberOfValues(this.valuesCount).buildIntersection();
            SketchAggregator.updateIntersection(intersection, lhs,SketchAggregator.max);
            SketchAggregator.updateIntersection(intersection, rhs,SketchAggregator.max);
            return intersection.getResult();
        }
        case FUNC_MIN:{
            ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().setNominalEntries(size).setNumberOfValues(this.valuesCount).buildIntersection();
            SketchAggregator.updateIntersection(intersection, lhs,SketchAggregator.min);
            SketchAggregator.updateIntersection(intersection, rhs,SketchAggregator.min);
            return intersection.getResult();
        }
        default:
            throw new IAE("not support this function "+this.valuesFunction);
        }

    }

    /**
     * Finalize the computation on sketch object and returns estimate from underlying
     * sketch.
     *
     * @param object the sketch object
     * @return sketch object
     */
    @Override
    public Object finalizeComputation(Object object) {
        if (shouldFinalize) {
             if(object instanceof ArrayOfDoublesSketch){
                 ArrayOfDoublesSketch tuple=(ArrayOfDoublesSketch)object;

                 ArrayOfDoublesSketchIterator it = tuple.iterator();
                 StringBuilder sb = new StringBuilder();
                 sb.append("{");
                 while(it.next()){
                     sb.append("[");
                     for(double v : it.getValues()){
                         sb.append(v+",");
                     }
                     if(sb.charAt(sb.length()-1) == ','){
                         sb.deleteCharAt(sb.length()-1);
                     }
                     sb.append("],");
                 }
                 if(sb.charAt(sb.length()-1) == ','){
                     sb.deleteCharAt(sb.length()-1);
                 }
                 sb.append("}");
                 return String.format("{\"total\":%s,\"theta\":%s,\"detail\":%s}",tuple.getEstimate(),tuple.getTheta(),sb.toString());
             }
             throw new IAE("not support this class "+object.getClass());
        } else {
            return object;
        }
    }

    @Override
    public String getTypeName() {
        if (isInputTupleSketch) {
            return SketchModule.TUPLE_DOUBLE_SKETCH_MERGE_AGG;
        } else {
            return SketchModule.TUPLE_DOUBLE_SKETCH_BUILD_AGG;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        SketchMergeAggregatorFactory that = (SketchMergeAggregatorFactory) o;

        if (shouldFinalize != that.shouldFinalize) {
            return false;
        }
        if (valuesCount != that.valuesCount) {
            return false;
        }
        if (!valuesFunction.equals(that.valuesFunction)){
            return false;
        }
        return isInputTupleSketch == that.isInputTupleSketch;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (shouldFinalize ? 1 : 0);
        result = 31 * result + (isInputTupleSketch ? 1 : 0);
        result = 31 * result + valuesCount;
        result = 31 * result + valuesFunction.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SketchMergeAggregatorFactory{"
                 + "fieldName=" + fieldName
                + ", fieldNames=" + fieldNames
                + ", name=" + name
                + ", size=" + size
                + ", shouldFinalize=" + shouldFinalize
                + ", isInputTupleSketch=" + isInputTupleSketch
                + ", valuesCount="+valuesCount
                + ", valuesFunction="+valuesFunction
                + "}";
    }
}
