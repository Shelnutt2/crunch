@0xc325e4a2889b8af1;
struct T1 {
  nullColumns @0 :List(Bool);
  capnpSchemaVersion @1 :UInt64 = 1;
  column1 @2 :Int8;
  column2 @3 :Text;
}

const minimumCompatibleSchemaVersion :UInt64 = 1;