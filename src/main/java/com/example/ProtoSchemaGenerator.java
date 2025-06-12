package com.example;

import com.google.protobuf.DescriptorProtos;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

/**
 * ProtoSchemaGenerator generates Hive, Avro, and Iceberg schemas
 * from protobuf descriptor sets, supporting nested and repeated fields.
 */
public class ProtoSchemaGenerator {

    // Mapping from protobuf field types to Hive types
    private static final Map<Integer, String> PROTOBUF_TO_HIVE = createProtoToHiveMap();

    private static Map<Integer, String> createProtoToHiveMap() {
        Map<Integer, String> map = new HashMap<>();
        map.put(1, "DOUBLE");
        map.put(2, "FLOAT");
        map.put(3, "BIGINT");
        map.put(4, "BIGINT");
        map.put(5, "INT");
        map.put(6, "BIGINT");
        map.put(7, "INT");
        map.put(8, "BOOLEAN");
        map.put(9, "STRING");
        map.put(12, "BINARY");
        map.put(13, "INT");
        map.put(15, "INT");
        map.put(16, "BIGINT");
        map.put(17, "INT");
        map.put(18, "BIGINT");
        return Collections.unmodifiableMap(map);
    }

    // Mapping from protobuf field types to Avro types
    private static final Map<Integer, String> PROTOBUF_TO_AVRO = createProtoToAvroMap();

    private static Map<Integer, String> createProtoToAvroMap() {
        Map<Integer, String> map = new HashMap<>();
        map.put(1, "double");
        map.put(2, "float");
        map.put(3, "long");
        map.put(4, "long");
        map.put(5, "int");
        map.put(6, "long");
        map.put(7, "int");
        map.put(8, "boolean");
        map.put(9, "string");
        map.put(12, "bytes");
        map.put(13, "int");
        map.put(15, "int");
        map.put(16, "long");
        map.put(17, "int");
        map.put(18, "long");
        return Collections.unmodifiableMap(map);
    }

    // Iceberg uses similar types to Hive here
    private static final Map<Integer, String> PROTOBUF_TO_ICEBERG = createProtoToIcebergMap();

    private static Map<Integer, String> createProtoToIcebergMap() {
        Map<Integer, String> map = new HashMap<>();
        map.putAll(PROTOBUF_TO_HIVE);
        return Collections.unmodifiableMap(map);
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("Usage: java ProtoSchemaGenerator <desc_file> <message_fqname> <format> [--nested-struct | --no-nested]");
            System.err.println("Formats: hive | avro | iceberg");
            System.exit(1);
        }

        final String descFile = args[0];
        final String messageName = args[1];
        final String format = args[2].toLowerCase(Locale.ROOT);

        boolean supportNested = true;  // default true (flatten or nested struct)
        boolean nestedStruct = false;  // default flatten

        // Parse optional flags
        for (int i = 3; i < args.length; i++) {
            String opt = args[i];
            if (opt.equals("--nested-struct")) {
                nestedStruct = true;
                supportNested = true;
            } else if (opt.equals("--no-nested")) {
                supportNested = false;
                nestedStruct = false;
            } else if (opt.equals("--nested")) {
                supportNested = true;
                nestedStruct = false;
            } else {
                System.err.println("Unknown option: " + opt);
                System.exit(2);
            }
        }

        DescriptorProtos.FileDescriptorSet descriptorSet;
        try (FileInputStream fis = new FileInputStream(descFile)) {
            descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(fis);
        }

        // Build message map for lookup by full name
        Map<String, DescriptorProtos.DescriptorProto> messageMap = new HashMap<>();
        for (DescriptorProtos.FileDescriptorProto fdp : descriptorSet.getFileList()) {
            for (DescriptorProtos.DescriptorProto dp : fdp.getMessageTypeList()) {
                String fullName = fdp.getPackage().isEmpty() ? dp.getName() : fdp.getPackage() + "." + dp.getName();
                messageMap.put(fullName, dp);
                collectNestedMessages(fullName, dp, messageMap);
            }
        }

        DescriptorProtos.DescriptorProto message = messageMap.get(messageName);
        if (message == null) {
            System.err.println("Message not found: " + messageName);
            System.exit(3);
        }

        switch (format) {
            case "hive":
                System.out.println(generateHiveReplaceColumns(message, messageMap, supportNested, nestedStruct));
                break;
            case "avro":
                Map<String, Object> avroSchema = generateAvroSchema(message, messageMap, supportNested, nestedStruct);
                System.out.println(prettyJson(avroSchema));
                break;
            case "iceberg":
                System.out.println(generateIcebergCreateTable(message, messageMap, supportNested, nestedStruct));
                break;
            default:
                System.err.println("Unknown format: " + format);
                System.exit(4);
        }
    }

    private static void collectNestedMessages(String parentName, DescriptorProtos.DescriptorProto msg,
                                              Map<String, DescriptorProtos.DescriptorProto> messageMap) {
        for (DescriptorProtos.DescriptorProto nested : msg.getNestedTypeList()) {
            String nestedName = parentName + "." + nested.getName();
            messageMap.put(nestedName, nested);
            collectNestedMessages(nestedName, nested, messageMap);
        }
    }

    private static String generateHiveReplaceColumns(DescriptorProtos.DescriptorProto msg,
                                                     Map<String, DescriptorProtos.DescriptorProto> messageMap,
                                                     boolean supportNested,
                                                     boolean nestedStruct) {
        List<String> fields = generateIcebergFields(msg, messageMap, "", supportNested, nestedStruct);
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE your_table_name REPLACE COLUMNS (\n");
        for (int i = 0; i < fields.size(); i++) {
            sb.append("  ").append(fields.get(i));
            if (i != fields.size() - 1) sb.append(",");
            sb.append("\n");
        }
        sb.append(");");
        return sb.toString();
    }

    private static Map<String, Object> generateAvroSchema(DescriptorProtos.DescriptorProto msg,
                                                          Map<String, DescriptorProtos.DescriptorProto> messageMap,
                                                          boolean supportNested,
                                                          boolean nestedStruct) {
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "record");
        schema.put("name", msg.getName());
        schema.put("fields", generateAvroFields(msg, messageMap, supportNested, nestedStruct));
        return schema;
    }

    private static List<Object> generateAvroFields(DescriptorProtos.DescriptorProto msg,
                                                   Map<String, DescriptorProtos.DescriptorProto> messageMap,
                                                   boolean supportNested,
                                                   boolean nestedStruct) {
        List<Object> fields = new ArrayList<>();
        for (DescriptorProtos.FieldDescriptorProto field : msg.getFieldList()) {
            Map<String, Object> fieldDef = new LinkedHashMap<>();
            fieldDef.put("name", field.getName());

            boolean repeated = field.getLabel() == DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED;
            Object typeObj;

            if (field.getType() == DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE) {
                String typeName = field.getTypeName();
                if (typeName.startsWith(".")) typeName = typeName.substring(1);
                DescriptorProtos.DescriptorProto nested = messageMap.get(typeName);

                if (!supportNested) {
                    typeObj = "string"; // fallback when nested disabled
                } else if (nestedStruct) {
                    if (nested != null) {
                        Map<String, Object> nestedRecord = new LinkedHashMap<>();
                        nestedRecord.put("type", "record");
                        nestedRecord.put("name", simpleName(typeName));
                        nestedRecord.put("fields", generateAvroFields(nested, messageMap, supportNested, nestedStruct));
                        typeObj = nestedRecord;
                    } else {
                        typeObj = "string";
                    }
                } else {
                    // flatten nested as fields with underscores
                    if (nested != null) {
                        List<Object> nestedFields = generateAvroFields(nested, messageMap, supportNested, nestedStruct);
                        for (Object nf : nestedFields) {
                            if (nf instanceof Map<?, ?> nfMap) {
                                @SuppressWarnings("unchecked")
                                Map<String, Object> flatField = new LinkedHashMap<>((Map<String, Object>) nfMap);
                                String nestedFieldName = field.getName() + "_" + flatField.get("name");
                                flatField.put("name", nestedFieldName);
                                fields.add(flatField);
                            }
                        }
                        continue; // skip adding this field itself
                    } else {
                        typeObj = "string";
                    }
                }
            } else {
                typeObj = PROTOBUF_TO_AVRO.getOrDefault(field.getType().getNumber(), "string");
            }

            if (repeated) {
                Map<String, Object> arrayType = new LinkedHashMap<>();
                arrayType.put("type", "array");
                arrayType.put("items", typeObj);
                typeObj = arrayType;
            }

            fieldDef.put("type", typeObj);
            fields.add(fieldDef);
        }
        return fields;
    }

    private static List<String> generateIcebergFields(DescriptorProtos.DescriptorProto msg,
                                                      Map<String, DescriptorProtos.DescriptorProto> messageMap,
                                                      String prefix,
                                                      boolean supportNested,
                                                      boolean nestedStruct) {
        List<String> fields = new ArrayList<>();
        for (DescriptorProtos.FieldDescriptorProto field : msg.getFieldList()) {
            final String fieldName = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
            boolean repeated = field.getLabel() == DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED;
            String typeStr;

            if (field.getType() == DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE) {
                String typeName = field.getTypeName();
                if (typeName.startsWith(".")) typeName = typeName.substring(1);
                DescriptorProtos.DescriptorProto nested = messageMap.get(typeName);

                if (!supportNested) {
                    // flatten nested message fields with underscores
                    if (nested != null) {
                        List<String> nestedFields = generateIcebergFields(nested, messageMap, fieldName, supportNested, nestedStruct);
                        fields.addAll(nestedFields);
                        continue;
                    } else {
                        typeStr = "string";
                    }
                } else if (nestedStruct) {
                    if (nested != null) {
                        // Nested struct type in Iceberg: struct<field1:type1,...>
                        List<String> nestedFields = generateIcebergFields(nested, messageMap, "", supportNested, nestedStruct);
                        String nestedStructStr = "struct<" + String.join(",", nestedFields) + ">";
                        typeStr = nestedStructStr;
                    } else {
                        typeStr = "string";
                    }
                } else {
                    // Flatten nested fields with underscores
                    if (nested != null) {
                        List<String> nestedFields = generateIcebergFields(nested, messageMap, fieldName, supportNested, nestedStruct);
                        fields.addAll(nestedFields);
                        continue;
                    } else {
                        typeStr = "string";
                    }
                }
            } else {
                typeStr = PROTOBUF_TO_ICEBERG.getOrDefault(field.getType().getNumber(), "string");
            }

            if (repeated) {
                typeStr = "array<" + typeStr + ">";
            }

            fields.add(fieldName + " " + typeStr);
        }
        return fields;
    }

    private static String generateIcebergCreateTable(DescriptorProtos.DescriptorProto msg,
                                                     Map<String, DescriptorProtos.DescriptorProto> messageMap,
                                                     boolean supportNested,
                                                     boolean nestedStruct) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE your_table_name (\n");
        List<String> fields = generateIcebergFields(msg, messageMap, "", supportNested, nestedStruct);
        for (int i = 0; i < fields.size(); i++) {
            sb.append("  ").append(fields.get(i));
            if (i != fields.size() - 1) sb.append(",");
            sb.append("\n");
        }
        sb.append(")\nSTORED AS ICEBERG;");
        return sb.toString();
    }

    private static String simpleName(String fqName) {
        int lastDot = fqName.lastIndexOf('.');
        return lastDot >= 0 ? fqName.substring(lastDot + 1) : fqName;
    }

    // Simple pretty JSON printer for Map<String,Object> to print Avro schema
    private static String prettyJson(Object obj) {
        StringBuilder sb = new StringBuilder();
        prettyJson(obj, sb, 0);
        return sb.toString();
    }

    private static void prettyJson(Object obj, StringBuilder sb, int indent) {
        final String indentStr = "  ".repeat(indent);
        if (obj instanceof Map<?, ?> map) {
            sb.append("{\n");
            Iterator<? extends Map.Entry<?, ?>> it = map.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<?, ?> e = it.next();
                sb.append(indentStr).append("  \"").append(e.getKey()).append("\": ");
                prettyJson(e.getValue(), sb, indent + 1);
                if (it.hasNext()) sb.append(",");
                sb.append("\n");
            }
            sb.append(indentStr).append("}");
        } else if (obj instanceof List<?> list) {
            sb.append("[\n");
            Iterator<?> it = list.iterator();
            while (it.hasNext()) {
                sb.append(indentStr).append("  ");
                prettyJson(it.next(), sb, indent + 1);
                if (it.hasNext()) sb.append(",");
                sb.append("\n");
            }
            sb.append(indentStr).append("]");
        } else if (obj instanceof String s) {
            sb.append("\"").append(s).append("\"");
        } else {
            sb.append(String.valueOf(obj));
        }
    }
}
