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

import org.apache.commons.codec.binary.Base64;

import com.google.common.base.Charsets;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketches;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;

/**
 * 
 * @author sunxin@rongcapital.cn
 *
 */
public class SketchOperations {

	public static final ArrayOfDoublesSketch EMPTY_SKETCH = new ArrayOfDoublesUpdatableSketchBuilder()
			.setNominalEntries(16384).setResizeFactor(ResizeFactor.X2).setNumberOfValues(1).build();

	public static ArrayOfDoublesUpdatableSketch createSketch(int size, int valuesCount, Object key, Object values) {
		ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(size)
				.setResizeFactor(ResizeFactor.X2).setNumberOfValues(valuesCount).build();
		String KeyStr = (String) key;
		double[] valuesAry = (double[]) values;
		double[] finalValuesAry = new double[valuesCount];
		System.arraycopy(valuesAry, 0, finalValuesAry, 0, valuesAry.length);
		sketch.update(KeyStr, finalValuesAry);
		return sketch;
	}

	public static ArrayOfDoublesSketch deserialize(Object serializedSketch) {
		if (serializedSketch instanceof String) {
			return deserializeFromBase64EncodedString((String) serializedSketch);
		} else if (serializedSketch instanceof byte[]) {
			return deserializeFromByteArray((byte[]) serializedSketch);
		} else if (serializedSketch instanceof ArrayOfDoublesSketch) {
			return (ArrayOfDoublesSketch) serializedSketch;
		}

		throw new IllegalStateException(
				"Object is not of a type that can deserialize to sketch: " + serializedSketch.getClass());
	}

	public static ArrayOfDoublesSketch deserializeFromBase64EncodedString(String str) {
		return deserializeFromByteArray(Base64.decodeBase64(str.getBytes(Charsets.UTF_8)));
	}

	public static ArrayOfDoublesSketch deserializeFromByteArray(byte[] data) {
		return deserializeFromMemory(new NativeMemory(data));
	}

	public static ArrayOfDoublesSketch deserializeFromMemory(Memory mem) {
		if (Sketch.getSerializationVersion(mem) < 3) {
			return ArrayOfDoublesSketches.heapifySketch(mem);
		} else {
			return ArrayOfDoublesSketches.wrapSketch(mem);
		}
	}

}
