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

import com.yahoo.sketches.tuple.ArrayOfDoublesAnotB;
import com.yahoo.sketches.tuple.ArrayOfDoublesIntersection;
import com.yahoo.sketches.tuple.ArrayOfDoublesSetOperationBuilder;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesCombiner;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.ObjectColumnSelector;

import java.util.List;

/**
 * 
 * @author sunxin@rongcapital.cn
 *
 */
@SuppressWarnings({"rawtypes","unused"})
public abstract class SketchIntersectionAggregator extends SketchUnionAggregator {
    
    public SketchIntersectionAggregator(String name,ObjectColumnSelector selector,List<FloatColumnSelector> selectors,int size,int valuesCount) {
        super(name, selector, selectors, size, valuesCount);

    }

    @Override
    public void update(Object key) {
        ArrayOfDoublesSketch sketch = parseSketch(key);

        ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder().setNominalEntries(size).setNumberOfValues(valuesCount).buildIntersection();
        intersection.update(sketch, getCombiner());
        intersection.update(union.getResult(), getCombiner());

        ArrayOfDoublesAnotB anotb = new ArrayOfDoublesSetOperationBuilder().setNominalEntries(size).setNumberOfValues(valuesCount).buildAnotB();
        anotb.update(union.getResult(), sketch);

        ArrayOfDoublesAnotB bnota = new ArrayOfDoublesSetOperationBuilder().setNominalEntries(size).setNumberOfValues(valuesCount).buildAnotB();
        bnota.update(sketch,union.getResult());

        union.reset();
        union.update(intersection.getResult());
        union.update(anotb.getResult());
        union.update(bnota.getResult());

    }


    public abstract ArrayOfDoublesCombiner getCombiner();


}
