package db.engine.catalog;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class CatalogManager {
    private Map<String, TableSchema> tables = new HashMap<>();
    private Map<String, IndexSchema> indexes = new HashMap<>();

    private final File tablesFile = new File("catalog/tables.json");
    private final File indexesFile = new File("catalog/indexes.json");

    private final Gson gson = new Gson();

    public CatalogManager() {
        loadCatalog();
    }

    public void registerTable(TableSchema tSchema) {
        tables.put(tSchema.name(), tSchema);
        saveTables();
    }

    public TableSchema getTableSchema(String name) {
        return tables.get(name);
    }

    public void registerIndex(IndexSchema iSchema) {
        indexes.put(iSchema.name(), iSchema);
        saveIndexes();
    }

    public IndexSchema getIndexSchema(String name) {
        return indexes.get(name);
    }

    private void loadCatalog() {
        try {
            if (tablesFile.exists()) {
                tables = gson.fromJson(new FileReader(tablesFile),
                        new TypeToken<Map<String, TableSchema>>(){}.getType());
            }
            if (indexesFile.exists()) {
                indexes = gson.fromJson(new FileReader(indexesFile),
                        new TypeToken<Map<String, IndexSchema>>(){}.getType());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Ensure not null
        if (tables == null) tables = new HashMap<>();
        if (indexes == null) indexes = new HashMap<>();
    }

    // Could be used in the future
    private void saveCatalog() {
        saveTables();
        saveIndexes();
    }

    private void saveTables() {
        try (FileWriter tw = new FileWriter(tablesFile)) {
            gson.toJson(tables, tw);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void saveIndexes() {
        try (FileWriter iw = new FileWriter(indexesFile)) {
            gson.toJson(indexes, iw);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
