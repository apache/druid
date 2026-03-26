package tech.stackable.druid.opaauthorizer.opatypes;

public class OpaResource {
  public String name;
  public String type;

  public OpaResource(String name, String type) {
    this.name = name;
    this.type = type;
  }
}
