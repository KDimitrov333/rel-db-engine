package db.engine.catalog;

import java.util.List;

public class TableSchema {
    private String name;
    private List<ColumnSchema> columns;
    private String filePath;

    public TableSchema(String name, List<ColumnSchema> columns, String filePath) {
        this.name = name;
        this.columns = columns;
        this.filePath = filePath;
    }

    public String getName() {
        return name;
    }

    public List<ColumnSchema> getColumns() {
        return columns;
    }

    public String getFilePath() {
        return filePath;
    }
}