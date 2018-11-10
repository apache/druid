namespace java org.apache.druid.data.input.thrift

struct Author {
  1: string firstName;
  2: string lastName;
}

struct Book {
  1: string date;
  2: double price;
  3: string title;
  4: Author author;
}
