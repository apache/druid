/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.testing.embedded.tools;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A hand-written Thrift struct representing a Wikipedia stream event for use in
 * embedded stream ingestion tests. All fields are strings so that
 * {@code TSimpleJSONProtocol} can serialize them without special handling.
 */
public class WikipediaThriftEvent
    implements TBase<WikipediaThriftEvent, WikipediaThriftEvent._Fields>, java.io.Serializable
{
  private static final long serialVersionUID = 1L;

  private static final TStruct STRUCT_DESC = new TStruct("WikipediaThriftEvent");

  private static final TField TIMESTAMP_FIELD_DESC = new TField("timestamp", TType.STRING, (short) 1);
  private static final TField PAGE_FIELD_DESC = new TField("page", TType.STRING, (short) 2);
  private static final TField LANGUAGE_FIELD_DESC = new TField("language", TType.STRING, (short) 3);
  private static final TField USER_FIELD_DESC = new TField("user", TType.STRING, (short) 4);
  private static final TField UNPATROLLED_FIELD_DESC = new TField("unpatrolled", TType.STRING, (short) 5);
  private static final TField NEW_PAGE_FIELD_DESC = new TField("newPage", TType.STRING, (short) 6);
  private static final TField ROBOT_FIELD_DESC = new TField("robot", TType.STRING, (short) 7);
  private static final TField ANONYMOUS_FIELD_DESC = new TField("anonymous", TType.STRING, (short) 8);
  private static final TField NAMESPACE_FIELD_DESC = new TField("namespace", TType.STRING, (short) 9);
  private static final TField CONTINENT_FIELD_DESC = new TField("continent", TType.STRING, (short) 10);
  private static final TField COUNTRY_FIELD_DESC = new TField("country", TType.STRING, (short) 11);
  private static final TField REGION_FIELD_DESC = new TField("region", TType.STRING, (short) 12);
  private static final TField CITY_FIELD_DESC = new TField("city", TType.STRING, (short) 13);
  private static final TField ADDED_FIELD_DESC = new TField("added", TType.STRING, (short) 14);
  private static final TField DELETED_FIELD_DESC = new TField("deleted", TType.STRING, (short) 15);
  private static final TField DELTA_FIELD_DESC = new TField("delta", TType.STRING, (short) 16);

  public String timestamp;
  public String page;
  public String language;
  public String user;
  public String unpatrolled;
  public String newPage;
  public String robot;
  public String anonymous;
  public String namespace;
  public String continent;
  public String country;
  public String region;
  public String city;
  public String added;
  public String deleted;
  public String delta;

  public enum _Fields implements TFieldIdEnum
  {
    TIMESTAMP((short) 1, "timestamp"),
    PAGE((short) 2, "page"),
    LANGUAGE((short) 3, "language"),
    USER((short) 4, "user"),
    UNPATROLLED((short) 5, "unpatrolled"),
    NEW_PAGE((short) 6, "newPage"),
    ROBOT((short) 7, "robot"),
    ANONYMOUS((short) 8, "anonymous"),
    NAMESPACE((short) 9, "namespace"),
    CONTINENT((short) 10, "continent"),
    COUNTRY((short) 11, "country"),
    REGION((short) 12, "region"),
    CITY((short) 13, "city"),
    ADDED((short) 14, "added"),
    DELETED((short) 15, "deleted"),
    DELTA((short) 16, "delta");

    private static final Map<String, _Fields> BY_NAME = new HashMap<>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        BY_NAME.put(field.getFieldName(), field);
      }
    }

    public static _Fields findByThriftId(int fieldId)
    {
      switch (fieldId) {
        case 1:
          return TIMESTAMP;
        case 2:
          return PAGE;
        case 3:
          return LANGUAGE;
        case 4:
          return USER;
        case 5:
          return UNPATROLLED;
        case 6:
          return NEW_PAGE;
        case 7:
          return ROBOT;
        case 8:
          return ANONYMOUS;
        case 9:
          return NAMESPACE;
        case 10:
          return CONTINENT;
        case 11:
          return COUNTRY;
        case 12:
          return REGION;
        case 13:
          return CITY;
        case 14:
          return ADDED;
        case 15:
          return DELETED;
        case 16:
          return DELTA;
        default:
          return null;
      }
    }

    public static _Fields findByName(String name)
    {
      return BY_NAME.get(name);
    }

    private final short thriftId;
    private final String fieldName;

    _Fields(short thriftId, String fieldName)
    {
      this.thriftId = thriftId;
      this.fieldName = fieldName;
    }

    @Override
    public short getThriftFieldId()
    {
      return thriftId;
    }

    @Override
    public String getFieldName()
    {
      return fieldName;
    }
  }

  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> META_DATA_MAP;

  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<>(_Fields.class);
    for (_Fields field : _Fields.values()) {
      tmpMap.put(field, new org.apache.thrift.meta_data.FieldMetaData(
          field.getFieldName(),
          org.apache.thrift.TFieldRequirementType.DEFAULT,
          new org.apache.thrift.meta_data.FieldValueMetaData(TType.STRING)
      ));
    }
    META_DATA_MAP = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(WikipediaThriftEvent.class, META_DATA_MAP);
  }

  public WikipediaThriftEvent()
  {
  }

  public WikipediaThriftEvent(WikipediaThriftEvent other)
  {
    this.timestamp = other.timestamp;
    this.page = other.page;
    this.language = other.language;
    this.user = other.user;
    this.unpatrolled = other.unpatrolled;
    this.newPage = other.newPage;
    this.robot = other.robot;
    this.anonymous = other.anonymous;
    this.namespace = other.namespace;
    this.continent = other.continent;
    this.country = other.country;
    this.region = other.region;
    this.city = other.city;
    this.added = other.added;
    this.deleted = other.deleted;
    this.delta = other.delta;
  }

  @Override
  public WikipediaThriftEvent deepCopy()
  {
    return new WikipediaThriftEvent(this);
  }

  @Override
  public void clear()
  {
    timestamp = null;
    page = null;
    language = null;
    user = null;
    unpatrolled = null;
    newPage = null;
    robot = null;
    anonymous = null;
    namespace = null;
    continent = null;
    country = null;
    region = null;
    city = null;
    added = null;
    deleted = null;
    delta = null;
  }

  @Override
  public _Fields fieldForId(int fieldId)
  {
    return _Fields.findByThriftId(fieldId);
  }

  @Override
  public boolean isSet(_Fields field)
  {
    if (field == null) {
      throw new IllegalArgumentException();
    }
    return getFieldValue(field) != null;
  }

  @Override
  public Object getFieldValue(_Fields field)
  {
    switch (field) {
      case TIMESTAMP:
        return timestamp;
      case PAGE:
        return page;
      case LANGUAGE:
        return language;
      case USER:
        return user;
      case UNPATROLLED:
        return unpatrolled;
      case NEW_PAGE:
        return newPage;
      case ROBOT:
        return robot;
      case ANONYMOUS:
        return anonymous;
      case NAMESPACE:
        return namespace;
      case CONTINENT:
        return continent;
      case COUNTRY:
        return country;
      case REGION:
        return region;
      case CITY:
        return city;
      case ADDED:
        return added;
      case DELETED:
        return deleted;
      case DELTA:
        return delta;
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public void setFieldValue(_Fields field, Object value)
  {
    switch (field) {
      case TIMESTAMP:
        timestamp = (String) value;
        break;
      case PAGE:
        page = (String) value;
        break;
      case LANGUAGE:
        language = (String) value;
        break;
      case USER:
        user = (String) value;
        break;
      case UNPATROLLED:
        unpatrolled = (String) value;
        break;
      case NEW_PAGE:
        newPage = (String) value;
        break;
      case ROBOT:
        robot = (String) value;
        break;
      case ANONYMOUS:
        anonymous = (String) value;
        break;
      case NAMESPACE:
        namespace = (String) value;
        break;
      case CONTINENT:
        continent = (String) value;
        break;
      case COUNTRY:
        country = (String) value;
        break;
      case REGION:
        region = (String) value;
        break;
      case CITY:
        city = (String) value;
        break;
      case ADDED:
        added = (String) value;
        break;
      case DELETED:
        deleted = (String) value;
        break;
      case DELTA:
        delta = (String) value;
        break;
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public void read(TProtocol iprot) throws TException
  {
    TField field;
    iprot.readStructBegin();
    while (true) {
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) {
        break;
      }
      if (field.type == TType.STRING) {
        switch (field.id) {
          case 1:
            timestamp = iprot.readString();
            break;
          case 2:
            page = iprot.readString();
            break;
          case 3:
            language = iprot.readString();
            break;
          case 4:
            user = iprot.readString();
            break;
          case 5:
            unpatrolled = iprot.readString();
            break;
          case 6:
            newPage = iprot.readString();
            break;
          case 7:
            robot = iprot.readString();
            break;
          case 8:
            anonymous = iprot.readString();
            break;
          case 9:
            namespace = iprot.readString();
            break;
          case 10:
            continent = iprot.readString();
            break;
          case 11:
            country = iprot.readString();
            break;
          case 12:
            region = iprot.readString();
            break;
          case 13:
            city = iprot.readString();
            break;
          case 14:
            added = iprot.readString();
            break;
          case 15:
            deleted = iprot.readString();
            break;
          case 16:
            delta = iprot.readString();
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
            break;
        }
      } else {
        TProtocolUtil.skip(iprot, field.type);
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();
    validate();
  }

  @Override
  public void write(TProtocol oprot) throws TException
  {
    validate();
    oprot.writeStructBegin(STRUCT_DESC);
    writeStringField(oprot, TIMESTAMP_FIELD_DESC, timestamp);
    writeStringField(oprot, PAGE_FIELD_DESC, page);
    writeStringField(oprot, LANGUAGE_FIELD_DESC, language);
    writeStringField(oprot, USER_FIELD_DESC, user);
    writeStringField(oprot, UNPATROLLED_FIELD_DESC, unpatrolled);
    writeStringField(oprot, NEW_PAGE_FIELD_DESC, newPage);
    writeStringField(oprot, ROBOT_FIELD_DESC, robot);
    writeStringField(oprot, ANONYMOUS_FIELD_DESC, anonymous);
    writeStringField(oprot, NAMESPACE_FIELD_DESC, namespace);
    writeStringField(oprot, CONTINENT_FIELD_DESC, continent);
    writeStringField(oprot, COUNTRY_FIELD_DESC, country);
    writeStringField(oprot, REGION_FIELD_DESC, region);
    writeStringField(oprot, CITY_FIELD_DESC, city);
    writeStringField(oprot, ADDED_FIELD_DESC, added);
    writeStringField(oprot, DELETED_FIELD_DESC, deleted);
    writeStringField(oprot, DELTA_FIELD_DESC, delta);
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  private static void writeStringField(TProtocol oprot, TField desc, String value) throws TException
  {
    if (value != null) {
      oprot.writeFieldBegin(desc);
      oprot.writeString(value);
      oprot.writeFieldEnd();
    }
  }

  public void validate() throws TException
  {
    // no required fields
  }

  @Override
  public int compareTo(WikipediaThriftEvent other)
  {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }
    return Objects.compare(timestamp, other.timestamp, java.util.Comparator.nullsFirst(java.util.Comparator.naturalOrder()));
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WikipediaThriftEvent that = (WikipediaThriftEvent) o;
    return Objects.equals(timestamp, that.timestamp)
           && Objects.equals(page, that.page)
           && Objects.equals(language, that.language)
           && Objects.equals(user, that.user)
           && Objects.equals(namespace, that.namespace);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(timestamp, page, language, user, namespace);
  }

  @Override
  public String toString()
  {
    return "WikipediaThriftEvent{timestamp=" + timestamp + ", page=" + page + "}";
  }
}
