namespace java io.druid.data.input.test

struct TestThriftObj
{
  1: required i32 id,
  2: optional string name,
  3: optional i64 timestamp,
  4: optional list<string> blackList,
}

