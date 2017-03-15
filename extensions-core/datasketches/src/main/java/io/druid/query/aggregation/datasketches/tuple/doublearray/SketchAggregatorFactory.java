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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;

import io.druid.query.aggregation.AggregatorFactory;

/**
 * 
 * @author sunxin@rongcapital.cn
 *
 */
public abstract class SketchAggregatorFactory extends AggregatorFactory {
    public static final int DEFAULT_MAX_SKETCH_SIZE = 16384;

    protected final String name;
    protected final String fieldName;
    protected final List<String> fieldNames;
    protected final int size;
    private final byte cacheId;

    public static final Comparator<ArrayOfDoublesSketch> COMPARATOR = new Comparator<ArrayOfDoublesSketch>() {
        @Override
        public int compare(ArrayOfDoublesSketch o, ArrayOfDoublesSketch o1) {
            return Doubles.compare(o.getEstimate(), o1.getEstimate());
        }
    };

    public SketchAggregatorFactory(String name, String fieldName,List<String> fieldNames, Integer size, byte cacheId) {
        this.name = Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
        this.fieldName = Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");
        this.fieldNames = Preconditions.checkNotNull(fieldNames, "Must have a valid, non-null fieldNames");

        this.size = size == null ? DEFAULT_MAX_SKETCH_SIZE : size;
        Util.checkIfPowerOf2(this.size, "size");

        this.cacheId = cacheId;
    }



    @Override
    public Object deserialize(Object object) {
        return SketchOperations.deserialize(object);
    }

    @Override
    public Comparator<ArrayOfDoublesSketch> getComparator() {
        return COMPARATOR;
    }

    
    @Override
    @JsonProperty
    public String getName() {
        return name;
    }
    
    @JsonProperty
    public String getFieldName(){
      return fieldName;
    }

    @JsonProperty
    public List<String> getFieldNames() {
        return fieldNames;
    }

    @JsonProperty
    public int getSize() {
        return size;
    }


    @Override
    public List<String> requiredFields() {
        return Collections.singletonList(fieldName);
    }

    @Override
    public byte[] getCacheKey() {
        byte[] fieldNameBytes = fieldName.getBytes();
        return ByteBuffer.allocate(1 + Ints.BYTES + fieldNameBytes.length)
                         .put(cacheId)
                         .putInt(size)
                         .put(fieldNameBytes)
                         .array();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{"
        		+ "fieldName='" + fieldName + '\''
                + ", fieldNames='" + fieldNames + '\''
                + ", name='" + name + '\''
                + ", size=" + size
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SketchAggregatorFactory that = (SketchAggregatorFactory) o;

        if (size != that.size) {
            return false;
        }
        if (cacheId != that.cacheId) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        if(!fieldName.equals(that.fieldName)){
        	return false;
        }
        return fieldNames.equals(that.fieldNames);

    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + fieldName.hashCode();
        result = 31 * result + fieldNames.hashCode();
        result = 31 * result + size;
        result = 31 * result + (int) cacheId;
        return result;
    }
}
