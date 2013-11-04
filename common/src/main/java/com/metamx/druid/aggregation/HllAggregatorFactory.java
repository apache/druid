package com.metamx.druid.aggregation;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.nio.ByteBuffer;

import org.apache.commons.codec.binary.Base64;

import com.google.common.base.Preconditions;
import com.metamx.common.logger.Logger;
import com.metamx.druid.processing.ColumnSelectorFactory;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import gnu.trove.map.hash.TIntByteHashMap;

public class HllAggregatorFactory implements AggregatorFactory {

	private final String fieldName;
	private final String name;

	static final int NUMARR = 7;
	static final int ARRLEN = 41;
	public static final int HLL_LOBINS = 1024;
	public static final int HLL_HIBINS = 65536;
	private static final Logger log = new Logger(HllAggregatorFactory.class);

	@JsonCreator
	public HllAggregatorFactory(@JsonProperty("name") String name,
			@JsonProperty("fieldName") final String fieldName) {
		Preconditions.checkNotNull(name,
				"Must have a valid, non-null aggregator name");
		Preconditions.checkNotNull(fieldName,
				"Must have a valid, non-null fieldName");

		this.name = name;
		this.fieldName = fieldName;
	}

	@Override
	public Aggregator factorize(ColumnSelectorFactory metricFactory) {
		return new HllAggregator(name,
				metricFactory.makeComplexMetricSelector(fieldName));
	}

	@Override
	public BufferAggregator factorizeBuffered(
			ColumnSelectorFactory metricFactory) {
		return new HllBufferAggregator(
				metricFactory.makeObjectColumnSelector(fieldName));
	}

	@Override
	public Comparator getComparator() {
		return HllAggregator.COMPARATOR;
	}

	@Override
	public Object combine(Object lhs, Object rhs) {
		return HllAggregator.combineHll(lhs, rhs);
	}

	@Override
	public AggregatorFactory getCombiningFactory() {
		log.info("factory name:"+name);
		return new HllAggregatorFactory(name, name);
	}

	@Override
	public Object deserialize(Object object) {
		
		
		log.info("class name:"+object.getClass()+":value "+object);
		
		String k = (String) object;
		byte[] ibmapByte = Base64.decodeBase64(k);

		TIntByteHashMap newIbMap;
		ByteBuffer buffer = ByteBuffer.wrap(ibmapByte);
		int keylength = buffer.getInt();
		int valuelength = buffer.getInt();
		if (keylength == 0) {
			newIbMap = new TIntByteHashMap();
		} else {
			int[] keys = new int[keylength];
			byte[] values = new byte[valuelength];

			for (int i = 0; i < keylength; i++) {
				keys[i] = buffer.getInt();
			}
			buffer.get(values);

			newIbMap = new TIntByteHashMap(keys, values);
		}
		
		return newIbMap;
	}

	@Override
	public Object finalizeComputation(Object object) {
		TIntByteHashMap ibMap = (TIntByteHashMap) object;
		int[] keys = ibMap.keys();
		double registerSum = 0;
		int count = keys.length;
		double zeros = 0.0;
		for (int i = 0; i < keys.length; i++) {
			{
				int val = ibMap.get(keys[i]);
				registerSum += 1.0 / (1 << val);
				if (val == 0) {
					zeros++;
				}
			}

		}
		registerSum += (HllAggregator.m - count);
		zeros += HllAggregator.m - count;

		double estimate = HllAggregator.alphaMM * (1.0 / registerSum);

		if (estimate <= (5.0 / 2.0) * (HllAggregator.m)) {
			// Small Range Estimate
			return Math.round(HllAggregator.m
					* Math.log(HllAggregator.m / zeros));
		} else {
			return Math.round(estimate);
		}
	}

	@JsonProperty
	public String getFieldName() {
		return fieldName;
	}

	@Override
	@JsonProperty
	public String getName() {
		return name;
	}

	@Override
	public List<String> requiredFields() {
		return Arrays.asList(fieldName);
	}

	@Override
	public byte[] getCacheKey() {

		byte[] fieldNameBytes = fieldName.getBytes();
		return ByteBuffer.allocate(1 + fieldNameBytes.length).put((byte) 0x37)
				.put(fieldNameBytes).array();
	}

	@Override
	public String getTypeName() {
		return "hll";
	}

	@Override
	public int getMaxIntermediateSize() {
		return HllAggregator.m;
	}

	@Override
	public Object getAggregatorStartValue() {
		return new TIntByteHashMap();
	}

}
