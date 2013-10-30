package com.metamx.druid.aggregation;

import com.metamx.druid.processing.ObjectColumnSelector;

import com.metamx.druid.processing.ComplexMetricSelector;

import java.nio.ByteBuffer;
import com.metamx.common.logger.Logger;
import java.io.PrintWriter;
import java.io.StringWriter;

import gnu.trove.map.hash.TIntByteHashMap;
import gnu.trove.map.TIntByteMap ;
import gnu.trove.procedure.TIntByteProcedure;

public class HllBufferAggregator implements BufferAggregator {
	private static final Logger log = new Logger(HllBufferAggregator.class);

  private final ObjectColumnSelector selector;
  private static long time=0;
  public HllBufferAggregator(ObjectColumnSelector selector) {
    this.selector = selector;
  }

  /*
 * byte 1 key length
 * byte 2 value length
 * byte 3...n key array
 * byte n+1.... value array
 */
  @Override
  public void init(ByteBuffer buf, int position) {
    for (int i =0; i<HllAggregator.m ; i++) {
      buf.put(position + i, (byte)0);
    }
  }

  @Override
  public void aggregate(ByteBuffer buf, int position) {
    final ByteBuffer fb = buf;
    final int fp = position;
    // TODO Auto-generated method stub
    TIntByteHashMap newobj = (TIntByteHashMap) (selector.get());
    newobj.forEachEntry(new TIntByteProcedure() {
        public boolean execute(int a, byte b) {
          if (b > fb.get(fp+a)) {
            fb.put(fp+a,b);
          }
          return true;
        }
      });
  }

  @Override
  public Object get(ByteBuffer buf, int position) {
    TIntByteHashMap ret = new TIntByteHashMap();
    for (int i =0;i<HllAggregator.m;i++)
    {
      if (buf.get(position + i) != 0){
        ret.put(i,buf.get(position + i));
      }
    }
    return ret;
  }

  @Override
    public float getFloat(ByteBuffer buf, int position) {
      throw new UnsupportedOperationException(
          "HllAggregator does not support getFloat()");
    }

  @Override
    public void close() {
    }
}

