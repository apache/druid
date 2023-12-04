package org.apache.druid.server.audit;

import org.apache.druid.java.util.common.logger.Logger;

public class AuditLogger
{
  public enum Level
  {
    DEBUG, INFO, WARN
  }

  private static final String MSG_FORMAT
      = "[%s] User[%s], ip[%s] performed action[%s] on key[%s] with comment[%s]. Payload[%s].";

  private final Level level;
  private final Logger logger = new Logger(AuditLogger.class);

  public AuditLogger(Level level)
  {
    this.level = level;
  }

  public void log(AuditRecord record)
  {
    Object[] args = {
        record.getAuditTime(),
        record.getAuditInfo().getAuthor(),
        record.getAuditInfo().getIp(),
        record.getType(),
        record.getKey(),
        record.getAuditInfo().getComment(),
        record.getPayload()
    };
    switch (level) {
      case DEBUG:
        logger.debug(MSG_FORMAT, args);
        break;
      case INFO:
        logger.info(MSG_FORMAT, args);
        break;
      case WARN:
        logger.warn(MSG_FORMAT, args);
        break;
    }
  }
}
