package com.metamx.druid.aggregation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.metamx.druid.aggregation.JavaScriptAggregator.ScriptAggregator;
import com.metamx.druid.processing.ObjectColumnSelector;
import com.metamx.common.logger.Logger;
import com.google.common.hash.Hashing;

import java.util.AbstractList;

import gnu.trove.map.hash.TIntByteHashMap;
import gnu.trove.map.TIntByteMap ;
import java.nio.ByteBuffer;

import java.net.InetAddress;
import org.apache.commons.codec.binary.Base64;
import com.metamx.druid.aggregation.HllAggregatorFactory;


public class HllAggregator implements Aggregator {

  private final String name;
  private final ObjectColumnSelector selector;
  private TIntByteHashMap ibMap;
  private static final Logger log = new Logger(HllAggregator.class);
  public static final int log2m = 12;
  public static final double alphaMM;
  public static final int m;
  private static long time = 0;	
  static{
    m = (int) Math.pow(2, log2m);
    alphaMM = (0.7213 / (1 + 1.079 / m)) * m * m;
  }

  static final Comparator COMPARATOR = new Comparator() {
    @Override
    public int compare(Object o, Object o1) {
      if(((TIntByteHashMap)o).equals((TIntByteHashMap)o1)){
        return 0;
      }else{
        return 1;
      }
    }
  };

  static Object combineHll(Object lhs, Object rhs) {
    TIntByteMap newIbMap = new TIntByteHashMap((TIntByteMap)lhs);
    TIntByteMap rightIbMap = (TIntByteMap)rhs;
    int[] keys = rightIbMap.keys();
    for(int i=0;i<keys.length;i++){
      int ii = keys[i];
      if (newIbMap.get(ii) == newIbMap.getNoEntryValue() || rightIbMap.get(ii) > newIbMap.get(ii)) {
        newIbMap.put(ii,rightIbMap.get(ii));
      }
    }
    return newIbMap;
  }

  public HllAggregator(String name, ObjectColumnSelector selector) {
    log.info("HllAggregator");
    this.name = name;
    this.selector = selector;
    this.ibMap = new TIntByteHashMap();
  }

  @Override
    public void aggregate() {

      long start = System.nanoTime();
      Object value = selector.get();
      log.info("byte: %s{}" , value.getClass());
        log.info("serde type:"+HllAggregatorFactory.context+"complex:"+HllAggregatorFactory.CONTEXT.COMPLEX);
        if (HllAggregatorFactory.context == HllAggregatorFactory.CONTEXT.COMPLEX) {

          String k = (String)((AbstractList) value).get(0);
          //log.info("input value {%s}", k);
          byte[] ibmapByte =  Base64.decodeBase64(k);

          //for(int i=0;i<ibmapByte.length;i++){

          //log.info("byte index:{%s},byte value:{%s}", i, ibmapByte[i]);
          //}
          TIntByteHashMap newIbMap;
          ByteBuffer buffer = ByteBuffer.wrap(ibmapByte);
          int keylength = buffer.getInt();
          int valuelength = buffer.getInt();
          if (keylength == 0) {
            newIbMap =	new TIntByteHashMap();
          }else{
            int[] keys = new int[keylength];
            byte[] values = new byte[valuelength];

            for (int i = 0; i < keylength; i++) {
              keys[i] = buffer.getInt();
            }
            // buffer.asIntBuffer().get(keys);
            // buffer.position+=4*keyength;
            buffer.get(values);

            newIbMap = new TIntByteHashMap(keys, values);
          }
          //TIntByteHashMap newIbMap = (TIntByteHashMap)((AbstractList) value).get(0);
          log.info("current value {%s}", value);
          int[] indexes = newIbMap.keys();
          log.info("new ib {%s} new ib values {%s}",indexes,newIbMap.values());
          log.info("byte: enter");
          for (int i = 0; i < indexes.length; i++) {
            int index_i = indexes[i];
            if (ibMap.get(index_i) == ibMap.getNoEntryValue() || newIbMap.get(index_i) > ibMap.get(index_i)) {
              ibMap.put(index_i,newIbMap.get(index_i));
            }
          }
        } else {
          log.info("string");
          log.info("aggregator");
          long id = Hashing.murmur3_128().hashString((String)((AbstractList)selector.get()).get(0)).asLong();
          //long id = Long.valueOf((String)((AbstractList)selector.get()).get(0));

          final int bucket = (int) (id >>> (Long.SIZE - log2m));
          final int zerolength = Long.numberOfLeadingZeros((id << this.log2m) | (1 << (this.log2m - 1)) + 1) + 1;


          if(ibMap.get(bucket) == ibMap.getNoEntryValue() || ibMap.get(bucket)<(byte)zerolength)
            ibMap.put(bucket,(byte)zerolength);

        }
        int[] indexes = ibMap.keys();
        for (int i = 0; i < indexes.length; i++) {
          int index_i = indexes[i];
        }
    }

  @Override
    public void reset() {
      this.ibMap = new TIntByteHashMap();

    }

  @Override
    public Object get() {
      log.info("HllAggregator get");
      // TODO Auto-generated method stub
      for (int i = 0; i < m; i++) {
      }
      return ibMap;
    }

  @Override
    public float getFloat() {
      // TODO Auto-generated method stub
      throw new UnsupportedOperationException(
          "Hll does not support getFloat()");
    }

  @Override
    public String getName() {
      log.info("HllAggregator getName");
      // TODO Auto-generated method stub
      return name;
    }

  @Override
    public void close() {
      // TODO Auto-generated method stub

    }

}

