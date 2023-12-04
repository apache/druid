package org.apache.druid.server.audit;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.audit.AuditManagerConfig;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.HumanReadableBytesRange;

public class LoggingAuditManagerConfig implements AuditManagerConfig
{
  @JsonProperty
  private final boolean includePayloadAsDimensionInMetric = false;

  @JsonProperty
  private final AuditLogger.Level logLevel = AuditLogger.Level.INFO;

  @JsonProperty
  @HumanReadableBytesRange(
      min = -1,
      message = "maxPayloadSizeBytes must either be -1 (for disabling the check) or a non negative number"
  )
  private final HumanReadableBytes maxPayloadSizeBytes = HumanReadableBytes.valueOf(-1);

  @JsonProperty
  private final boolean skipNullField = false;

  @Override
  public boolean isSkipNullField()
  {
    return skipNullField;
  }

  @Override
  public long getMaxPayloadSizeBytes()
  {
    return maxPayloadSizeBytes.getBytes();
  }

  public AuditLogger.Level getLogLevel()
  {
    return logLevel;
  }
}
