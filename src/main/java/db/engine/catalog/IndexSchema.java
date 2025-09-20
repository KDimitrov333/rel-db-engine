package db.engine.catalog;

public class IndexSchema {
    private String name;
    private String table;
    private String column;
    private String filePath;

    public IndexSchema(String name, String table, String column, String filePath) {
        this.name = name;
        this.table = table;
        this.column = column;
        this.filePath = filePath;
    }

    public String getName() {
        return name;
    }

    public String getTable() {
        return table;
    }

    public String getColumn() {
        return column;
    }

    public String getFilePath() {
        return filePath;
    }
}