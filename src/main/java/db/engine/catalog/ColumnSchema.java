package db.engine.catalog;

public class ColumnSchema {
    private String name;
    private String type;
    private int length;

    public ColumnSchema(String name, String type, int length) {
        this.name = name;
        this.type = type;
        this.length = length;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public int getLength() {
        return length;
    }
}