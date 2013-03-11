package com.metamx.druid.realtime;

import com.metamx.druid.client.DataSegment;

import java.io.IOException;

public class MetadataUpdater implements SegmentAnnouncer, SegmentPublisher
{
  private final SegmentAnnouncer segmentAnnouncer;
  private final SegmentPublisher segmentPublisher;

  public MetadataUpdater(SegmentAnnouncer segmentAnnouncer, SegmentPublisher segmentPublisher)
  {
    this.segmentAnnouncer = segmentAnnouncer;
    this.segmentPublisher = segmentPublisher;
  }

  @Override
  public void announceSegment(DataSegment segment) throws IOException
  {
    segmentAnnouncer.announceSegment(segment);
  }

  @Override
  public void unannounceSegment(DataSegment segment) throws IOException
  {
    segmentAnnouncer.unannounceSegment(segment);
  }

  @Override
  public void publishSegment(DataSegment segment) throws IOException
  {
    segmentPublisher.publishSegment(segment);
  }
}
