package db.engine.storage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import db.engine.catalog.ColumnSchema;

public class Record {
    private List<Object> values;

    public Record(List<Object> values) {
        this.values = values;
    }

    public List<Object> getValues() {
        return values;
    }

    // Serialize record to byte[]
    public byte[] toBytes(List<ColumnSchema> columns) {
        // Estimate buffer size (INT=4, BOOLEAN=1, VARCHAR = 4 (len) + bytes)
        int bufferSize = 0;
        for (int i = 0; i < columns.size(); i++) {
            ColumnSchema col = columns.get(i);
            Object val = values.get(i);
            switch (col.getType()) {
                case "INT":
                    bufferSize += 4;
                    break;
                case "BOOLEAN":
                    bufferSize += 1;
                    break;
                case "VARCHAR":
                    String s = (String) val;
                    bufferSize += 4 + s.getBytes(StandardCharsets.UTF_8).length;
                    break;
            }
        }

        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

        for (int i = 0; i < columns.size(); i++) {
            ColumnSchema col = columns.get(i);
            Object val = values.get(i);

            switch (col.getType()) {
                case "INT":
                    buffer.putInt((int) val);
                    break;
                case "BOOLEAN":
                    buffer.put((byte) ((Boolean) val ? 1 : 0));
                    break;
                case "VARCHAR":
                    byte[] strBytes = ((String) val).getBytes(StandardCharsets.UTF_8);
                    buffer.putInt(strBytes.length);
                    buffer.put(strBytes);
                    break;
            }
        }

        return buffer.array();
    }

    // Deserialize record from byte[]
    public static Record fromBytes(byte[] data, List<ColumnSchema> columns) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        List<Object> values = new ArrayList<>();

        for (ColumnSchema col : columns) {
            switch (col.getType()) {
                case "INT":
                    values.add(buffer.getInt());
                    break;
                case "BOOLEAN":
                    values.add(buffer.get() == 1);
                    break;
                case "VARCHAR":
                    int len = buffer.getInt();
                    byte[] strBytes = new byte[len];
                    buffer.get(strBytes);
                    values.add(new String(strBytes, StandardCharsets.UTF_8));
                    break;
            }
        }

        return new Record(values);
    }

    @Override
    public String toString() {
        return values.toString();
    }
}
