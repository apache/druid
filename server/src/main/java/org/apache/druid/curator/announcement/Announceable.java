package org.apache.druid.curator.announcement;

class Announceable
{
  final String path;
  final byte[] bytes;
  final boolean removeParentsIfCreated;

  public Announceable(String path, byte[] bytes, boolean removeParentsIfCreated)
  {
    this.path = path;
    this.bytes = bytes;
    this.removeParentsIfCreated = removeParentsIfCreated;
  }
}
