package org.apache.druid.msq.querykit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.segment.FrameSegment;
import org.apache.druid.frame.util.SettableLongVirtualColumn;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.java.util.common.Unit;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.input.table.SegmentWithDescriptor;
import org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor;
import org.apache.druid.query.operator.OffsetLimit;
import org.apache.druid.query.operator.Operator;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.operator.SegmentToRowsAndColumnsOperator;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.rowsandcols.LazilyDecoratedRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.FrameRowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.RowBasedFrameRowAndColumns;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.SimpleSettableOffset;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class WindowOperatorQueryFrameProcessor extends BaseLeafFrameProcessor
{

  private static final Logger log = new Logger(ScanQueryFrameProcessor.class);
  private final WindowOperatorQuery query;
  private final ObjectMapper jsonMapper;
  private final SettableLongVirtualColumn partitionBoostVirtualColumn;
  private final VirtualColumns frameWriterVirtualColumns;
  private final Closer closer = Closer.create();

  private Cursor cursor;
  private Segment segment;
  private final SimpleSettableOffset cursorOffset = new SimpleAscendingOffset(Integer.MAX_VALUE);
  private FrameWriter frameWriter;
  private long currentAllocatorCapacity; // Used for generating FrameRowTooLargeException if needed

  public WindowOperatorQueryFrameProcessor(
      final WindowOperatorQuery query,
      final ObjectMapper jsonMapper,
      final ReadableInput baseInput,
      final Function<SegmentReference, SegmentReference> segmentMapFn,
      final ResourceHolder<WritableFrameChannel> outputChannelHolder,
      final ResourceHolder<FrameWriterFactory> frameWriterFactoryHolder
  )
  {
    super(
        baseInput,
        segmentMapFn,
        outputChannelHolder,
        frameWriterFactoryHolder
    );
    this.query = query;
    this.jsonMapper = jsonMapper;
    this.partitionBoostVirtualColumn = new SettableLongVirtualColumn(QueryKitUtils.PARTITION_BOOST_COLUMN);

    final List<VirtualColumn> frameWriterVirtualColumns = new ArrayList<>();
    frameWriterVirtualColumns.add(partitionBoostVirtualColumn);

    final VirtualColumn segmentGranularityVirtualColumn =
        QueryKitUtils.makeSegmentGranularityVirtualColumn(jsonMapper, query);

    if (segmentGranularityVirtualColumn != null) {
      frameWriterVirtualColumns.add(segmentGranularityVirtualColumn);
    }

    this.frameWriterVirtualColumns = VirtualColumns.create(frameWriterVirtualColumns);
  }
  // deep storage
  @Override
  protected ReturnOrAwait<Unit> runWithSegment(SegmentWithDescriptor segment) throws IOException
  {
    return null;
  }

  // realtime
  @Override
  protected ReturnOrAwait<Unit> runWithLoadedSegment(SegmentWithDescriptor segment) throws IOException
  {
    return null;
  }

  // previous stage output
  @Override
  protected ReturnOrAwait<Unit> runWithInputChannel(ReadableFrameChannel inputChannel, FrameReader inputFrameReader)
      throws IOException
  {
    // Read the frames from the channel
    // convert to FrameRowsAndColumns

   if (inputChannel.canRead()) {
     Frame f = inputChannel.read();
     final FrameSegment frameSegment = new FrameSegment(f, inputFrameReader, SegmentId.dummy("x"));

     // the frame here is row based
     // frame rows and columns need columnar. discuss with Eric
     // Action item: need to implement a new rows and columns that accept a row-based frame
     RowBasedFrameRowAndColumns frameRowsAndColumns = new RowBasedFrameRowAndColumns(f, inputFrameReader.signature());
     LazilyDecoratedRowsAndColumns ldrc = new LazilyDecoratedRowsAndColumns(frameRowsAndColumns, null, null, null, OffsetLimit.limit(Integer.MAX_VALUE), null, null);
     // Create an operator on top of the created rows and columns
     Operator op = new Operator()
     {
       @Nullable
       @Override
       public Closeable goOrContinue(Closeable continuationObject, Receiver receiver)
       {
         receiver.push(ldrc);
         receiver.completed();
         return continuationObject;
       }
     };
     //
     //Operator op = new SegmentToRowsAndColumnsOperator(frameSegment);
     // On the operator created above add the operators present in the query that we want to chain

     for ( OperatorFactory of : query.getOperators()) {
       op = of.wrap(op);
     }

     // Let the operator run
     // the results that come in the receiver must be pushed to the outout channel
     // need to transform the output rows and columns back to frame
     Operator.go(op, new Operator.Receiver()
     {
       @Override
       public Operator.Signal push(RowsAndColumns rac)
       {
         //outputFrameChannel.output(rac.toFrame());
         return Operator.Signal.GO;
       }

       @Override
       public void completed()
       {

       }
     });

   } else if (inputChannel.isFinished()) {
     return ReturnOrAwait.returnObject(Unit.instance());
   } else {
     return ReturnOrAwait.awaitAll(inputChannels().size());
   }
    return ReturnOrAwait.runAgain();
  }
}
