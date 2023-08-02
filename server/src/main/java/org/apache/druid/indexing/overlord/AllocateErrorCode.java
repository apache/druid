package org.apache.druid.indexing.overlord;

import org.apache.druid.java.util.common.StringUtils;

public enum AllocateErrorCode
{
  /**
   * Message format = {@link Format#TASK_NOT_ACTIVE}.
   */
  TASK_NOT_ACTIVE(Format.TASK_NOT_ACTIVE),

  /**
   * Message format = {@link Format#LOCK_POSSE_NOT_FOUND}.
   */
  LOCK_POSSE_NOT_FOUND(Format.LOCK_POSSE_NOT_FOUND),

  /**
   * Message format = {@link Format#LOCK_REVOKED}.
   */
  LOCK_REVOKED(Format.LOCK_REVOKED),

  /**
   * Message format = {@link Format#LOCK_UPDATE_FAILED}.
   */
  LOCK_UPDATE_FAILED(Format.LOCK_UPDATE_FAILED),

  /**
   * Message format = {@link Format#MULTIPLE_CORE_PARTITION_SETS}.
   */
  MULTIPLE_CORE_PARTITION_SETS(Format.MULTIPLE_CORE_PARTITION_SETS),

  /**
   * Message format = {@link Format#UNKNOWN_NUM_CORE_PARTITIONS}.
   */
  UNKNOWN_NUM_CORE_PARTITIONS(Format.UNKNOWN_NUM_CORE_PARTITIONS);


  private final String msgFormat;

  AllocateErrorCode(String msgFormat)
  {
    this.msgFormat = msgFormat;
  }

  public String formatMsg(Object... args)
  {
    return StringUtils.format(msgFormat, args);
  }

  public static class Format
  {
    public static final String TASK_NOT_ACTIVE = "Task[%s] is not active anymore";
    public static final String LOCK_REVOKED = "Lock[%s] was revoked";
    public static final String LOCK_POSSE_NOT_FOUND = "Could not find or create lock posse for [%s]";
    public static final String LOCK_UPDATE_FAILED = "Could not update lock[%s] in metadata store";

    public static final String UNKNOWN_NUM_CORE_PARTITIONS = "";
    public static final String MULTIPLE_CORE_PARTITION_SETS = "";
    public static final String CONFLICTING_SEGMENT = "";
  }
}
