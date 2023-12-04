package org.apache.druid.server.audit;

import com.google.inject.Inject;
import org.apache.druid.audit.AuditEvent;
import org.apache.druid.audit.AuditManager;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.List;

/**
 * Audit manager that logs audited events at the level specified in
 * {@link LoggingAuditManagerConfig}.
 */
public class LoggingAuditManager implements AuditManager
{
  private final AuditLogger auditLogger;
  private final AuditSerdeHelper serdeHelper;

  @Inject
  public LoggingAuditManager(
      LoggingAuditManagerConfig config,
      AuditSerdeHelper serdeHelper
  )
  {
    this.serdeHelper = serdeHelper;
    this.auditLogger = new AuditLogger(config.getLogLevel());
  }

  @Override
  public void doAudit(AuditEvent event)
  {
    auditLogger.log(serdeHelper.processAuditEvent(event));
  }

  @Override
  public List<AuditEvent> fetchAuditHistory(String key, String type, Interval interval)
  {
    return Collections.emptyList();
  }

  @Override
  public List<AuditEvent> fetchAuditHistory(String type, Interval interval)
  {
    return Collections.emptyList();
  }

  @Override
  public List<AuditEvent> fetchAuditHistory(String key, String type, int limit)
  {
    return Collections.emptyList();
  }

  @Override
  public List<AuditEvent> fetchAuditHistory(String type, int limit)
  {
    return Collections.emptyList();
  }

  @Override
  public int removeAuditLogsOlderThan(long timestamp)
  {
    return 0;
  }
}
