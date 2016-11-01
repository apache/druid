namespace java io.druid.data.input.test

struct TestStruct {
  1: required i32 someInt;
}

enum TestEnum {
  ONE = 1,
  TWO = 2,
}

struct TestData {
  1: required i64 timestamp,
  2: optional i16 shortDimension,
  3: optional i32 intDimension,
  4: optional i64 longDimension,
  5: optional double doubleDimension,
  6: optional bool booleanDimension,
  7: optional string stringDimension,
  8: optional byte byteDimension,
  9: optional binary binaryDimension,
  10: optional TestEnum enumDimension,
  11: optional list<i32> intListDimension,
  12: optional set<i32> intSetDimension,
  13: optional map<i32, i32> intIntMapDimension,
  14: optional TestStruct structDimension,
  // 15 is skipped
  16: optional i16 shortMetrics,
  17: optional i32 intMetrics,
  18: optional i64 longMetrics,
  19: optional double doubleMetrics,
  20: optional string stringMetrics,
  21: optional byte byteMetrics,
}
