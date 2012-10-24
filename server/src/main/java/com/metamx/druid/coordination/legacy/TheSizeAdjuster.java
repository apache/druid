package com.metamx.druid.coordination.legacy;

import com.google.common.base.Joiner;
import com.metamx.common.MapUtils;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import org.I0Itec.zkclient.ZkClient;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 */
public class TheSizeAdjuster
{
  private static final Logger log = new Logger(TheSizeAdjuster.class);
  private static final Joiner JOINER = Joiner.on("/");

  private final TheSizeAdjusterConfig config;
  private final ObjectMapper jsonMapper;
  private final Map<String, SizeLookup> lookups;
  private final ZkClient zkClient;

  public TheSizeAdjuster(
      TheSizeAdjusterConfig config,
      ObjectMapper jsonMapper,
      Map<String, SizeLookup> lookups,
      ZkClient zkClient
  )
  {
    this.config = config;
    this.jsonMapper = jsonMapper;
    this.lookups = lookups;
    this.zkClient = zkClient;
  }

  public Long lookupSize(Map<String, Object> descriptor)
  {
    String type = MapUtils.getString(descriptor, "type");
    SizeLookup adjuster = lookups.get(type);

    if (adjuster == null) {
      log.warn("Unknown type[%s] for SizeAdjuster, known types are %s", type, lookups.keySet());
      return null;
    }

    return adjuster.lookupSize(descriptor);
  }

  public DataSegment updateDescriptor(DataSegment dataSegment)
  {
    Long size = lookupSize(dataSegment.getLoadSpec());

    if (size == null || size < 0) {
      log.warn("Unable to determine size[%s] of segment[%s], ignoring.", size, dataSegment);
      return null;
    }

    final DataSegment segment = new DataSegment(
        dataSegment.getDataSource(),
        dataSegment.getInterval(),
        dataSegment.getVersion() + "_w_size",
        dataSegment.getLoadSpec(),
        dataSegment.getDimensions(),
        dataSegment.getMetrics(),
        dataSegment.getShardSpec(),
        size
    );

    String oldSegmentPath = JOINER.join(config.getSegmentBasePath(), dataSegment.getDataSource(), dataSegment.getIdentifier());
    String newSegmentPath = JOINER.join(config.getSegmentBasePath(), segment.getDataSource(), segment.getIdentifier());
    try {
      String data = jsonMapper.writeValueAsString(segment);
      zkClient.createPersistent(newSegmentPath, data);
      log.info("Created new segment node[%s] with content[%s]", newSegmentPath, data);
      zkClient.delete(oldSegmentPath);
      log.info("Deleted old segment node[%s]", oldSegmentPath);
    }
    catch (IOException e) {
      log.warn(e, "Exception thrown on segment[%s]", segment);
      return null;
    }

    return segment;
  }
}
