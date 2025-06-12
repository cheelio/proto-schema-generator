#!/bin/bash

set -e

# CONFIG
PROTO_DIR="proto-example"
PROTO_FILE="example.proto"
DESC_FILE="example.desc"
MSG_NAME="analytics.UserEvent"
JAR_PATH="target/proto-schema-generator-1.0-SNAPSHOT.jar"

echo "🔧 Step 1: Setting up proto directory..."
mkdir -p "$PROTO_DIR"

cat > "$PROTO_DIR/$PROTO_FILE" <<EOF
syntax = "proto2";

package analytics;

enum DeviceType {
  UNKNOWN = 0;
  DESKTOP = 1;
  MOBILE = 2;
  TABLET = 3;
}

message Location {
  optional string city = 1;
  optional string country = 2;
  optional double latitude = 3;
  optional double longitude = 4;
}

message SessionInfo {
  required string session_id = 1;
  optional int64 start_time = 2;
  optional int64 end_time = 3;
  repeated string visited_pages = 4;
}

message Product {
  required string product_id = 1;
  required string name = 2;
  optional double price = 3;
  optional int32 quantity = 4;
}

message UserProfile {
  required string user_id = 1;
  optional string name = 2;
  optional string email = 3;
  optional bool is_premium = 4;
  repeated string interests = 5;
}

message UserEvent {
  required string event_id = 1;
  required int64 event_timestamp = 2;
  optional DeviceType device_type = 3;
  optional Location location = 4;
  optional SessionInfo session = 5;
  optional UserProfile profile = 6;
  repeated Product products = 7;
  repeated MetadataEntry metadata = 8;  // Replaces map field in proto2
}

// Proto2 does not support native maps — use a repeated key/value message instead
message MetadataEntry {
  required string key = 1;
  required string value = 2;
}
EOF

echo "✅ Proto file created at $PROTO_DIR/$PROTO_FILE"

# Check if protoc is available
if ! command -v protoc &> /dev/null; then
  echo "❌ 'protoc' is not installed. Install it from https://github.com/protocolbuffers/protobuf/releases"
  exit 1
fi

echo "📦 Step 2: Generating descriptor set..."
protoc --proto_path="$PROTO_DIR" \
       --descriptor_set_out="$DESC_FILE" \
       --include_imports \
       "$PROTO_DIR/$PROTO_FILE"

echo "✅ Descriptor set generated: $DESC_FILE"

echo "🚀 Step 3: Building Maven project..."
mvn clean package

echo "✅ JAR built: $JAR_PATH"

echo "📤 Step 4: Generating Hive schema..."
java -jar "$JAR_PATH" "$DESC_FILE" "$MSG_NAME" hive --nested-struct

echo "📤 Step 5: Generating Avro schema..."
java -jar "$JAR_PATH" "$DESC_FILE" "$MSG_NAME" avro --nested-struct

echo "📤 Step 6: Generating Iceberg schema..."
java -jar "$JAR_PATH" "$DESC_FILE" "$MSG_NAME" iceberg --nested-struct

echo "✅ All done!"
