package com.metamx.druid.aggregation;

import java.util.Comparator;
import com.metamx.druid.processing.ObjectColumnSelector;
import com.google.common.hash.Hashing;

import java.util.AbstractList;

import gnu.trove.map.hash.TIntByteHashMap;
import gnu.trove.map.TIntByteMap;
import java.nio.ByteBuffer;

import org.apache.commons.codec.binary.Base64;
import com.metamx.druid.aggregation.HllAggregatorFactory;

public class HllAggregator implements Aggregator {

	private final String name;
	private final ObjectColumnSelector selector;
	private TIntByteHashMap ibMap;
	public static final int log2m = 12;
	public static final double alphaMM;
	public static final int m;
	private static long time = 0;
	static {
		m = (int) Math.pow(2, log2m);
		alphaMM = (0.7213 / (1 + 1.079 / m)) * m * m;
	}

	static final Comparator COMPARATOR = new Comparator() {
		@Override
		public int compare(Object o, Object o1) {
			if (((TIntByteHashMap) o).equals((TIntByteHashMap) o1)) {
				return 0;
			} else {
				return 1;
			}
		}
	};

	static Object combineHll(Object lhs, Object rhs) {
		TIntByteMap newIbMap = new TIntByteHashMap((TIntByteMap) lhs);
		TIntByteMap rightIbMap = (TIntByteMap) rhs;
		int[] keys = rightIbMap.keys();
		for (int i = 0; i < keys.length; i++) {
			int ii = keys[i];
			if (newIbMap.get(ii) == newIbMap.getNoEntryValue()
					|| rightIbMap.get(ii) > newIbMap.get(ii)) {
				newIbMap.put(ii, rightIbMap.get(ii));
			}
		}
		return newIbMap;
	}

	public HllAggregator(String name, ObjectColumnSelector selector) {
		this.name = name;
		this.selector = selector;
		this.ibMap = new TIntByteHashMap();
	}

	@Override
	public void aggregate() {

		Object value = selector.get();
		if (HllAggregatorFactory.context == HllAggregatorFactory.CONTEXT.COMPLEX) {

			String k = (String) ((AbstractList) value).get(0);
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
			int[] indexes = newIbMap.keys();
			for (int i = 0; i < indexes.length; i++) {
				int index_i = indexes[i];
				if (ibMap.get(index_i) == ibMap.getNoEntryValue()
						|| newIbMap.get(index_i) > ibMap.get(index_i)) {
					ibMap.put(index_i, newIbMap.get(index_i));
				}
			}
		} else {
			long id = Hashing
					.murmur3_128()
					.hashString((String) ((AbstractList) selector.get()).get(0))
					.asLong();

			final int bucket = (int) (id >>> (Long.SIZE - log2m));
			final int zerolength = Long.numberOfLeadingZeros((id << log2m)
					| (1 << (log2m - 1)) + 1) + 1;

			if (ibMap.get(bucket) == ibMap.getNoEntryValue()
					|| ibMap.get(bucket) < (byte) zerolength)
				ibMap.put(bucket, (byte) zerolength);

		}
	}

	@Override
	public void reset() {
		this.ibMap = new TIntByteHashMap();

	}

	@Override
	public Object get() {
		return ibMap;
	}

	@Override
	public float getFloat() {
		throw new UnsupportedOperationException(
				"Hll does not support getFloat()");
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void close() {

	}

}
