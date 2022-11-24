package org.apache.druid.query.operator;

import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

import java.util.function.Supplier;

public class OperatorSequence implements Sequence<RowsAndColumns>
{
  private final Supplier<Operator> opSupplier;

  public OperatorSequence(
      Supplier<Operator> opSupplier
  )
  {
    this.opSupplier = opSupplier;
  }

  @Override
  public <OutType> OutType accumulate(
      OutType initValue, Accumulator<OutType, RowsAndColumns> accumulator
  )
  {
    Operator op = null;
    try {
      op = opSupplier.get();
      op.open();
      while (op.hasNext()) {
        initValue = accumulator.accumulate(initValue, op.next());
      }
      return initValue;
    }
    finally {
      if (op != null) {
        op.close(true);
      }
    }
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(
      OutType initValue, YieldingAccumulator<OutType, RowsAndColumns> accumulator
  )
  {
    final Operator op = opSupplier.get();
    try {
      op.open();

      while (!accumulator.yielded() && op.hasNext()) {
        initValue = accumulator.accumulate(initValue, op.next());
      }
      if (accumulator.yielded()) {
        OutType finalInitValue = initValue;
        return new Yielder<OutType>()
        {
          private OutType retVal = finalInitValue;
          private boolean done = false;

          @Override
          public OutType get()
          {
            return retVal;
          }

          @Override
          public Yielder<OutType> next(OutType initValue)
          {
            accumulator.reset();
            retVal = initValue;
            while (!accumulator.yielded() && op.hasNext()) {
              retVal = accumulator.accumulate(retVal, op.next());
            }
            if (!accumulator.yielded()) {
              done = true;
            }
            return this;
          }

          @Override
          public boolean isDone()
          {
            return done;
          }

          @Override
          public void close()
          {
            op.close(true);
          }
        };
      } else {
        return Yielders.done(initValue, () -> op.close(true));
      }
    }
    catch (RuntimeException e) {
      op.close(true);
      throw e;
    }
  }
}
