package org.apache.druid.msq.querykit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.util.SettableLongVirtualColumn;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.java.util.common.Unit;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.input.table.SegmentWithDescriptor;
import org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessor;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.SimpleSettableOffset;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;

import javax.annotation.Nullable;
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
    // Read the frames from the broker
    // convert to FrameRowsAndColumns
    // Find a way to call the operators on top of the frames
    // Find a way to flush the output to the outputChannel
    return null;
  }
}
