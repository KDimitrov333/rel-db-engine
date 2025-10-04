package db.engine.catalog;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class CatalogManager {
    private final Map<String, TableSchema> tables = new HashMap<>();
    private final Map<String, IndexSchema> indexes = new HashMap<>();

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
        loadInto(tablesFile, new TypeToken<Map<String, TableSchema>>(){}.getType(), tables);
        loadInto(indexesFile, new TypeToken<Map<String, IndexSchema>>(){}.getType(), indexes);
    }

    // Could be used in the future
    private void saveCatalog() {
        saveTables();
        saveIndexes();
    }

    private void saveTables() { writeMap(tables, tablesFile); }

    private void saveIndexes() { writeMap(indexes, indexesFile); }

    private <T> void loadInto(File file, Type type, Map<String, T> target) {
        if (!file.exists()) return;
        try (FileReader reader = new FileReader(file)) {
            Map<String, T> loaded = gson.fromJson(reader, type);
            if (loaded != null) {
                target.clear();
                target.putAll(loaded);
            }
        } catch (IOException | com.google.gson.JsonSyntaxException e) {
            logError("Failed loading catalog file: " + file.getPath(), e);
        }
    }

    private <T> void writeMap(Map<String, T> map, File file) {
        try (FileWriter writer = new FileWriter(file)) {
            gson.toJson(map, writer);
        } catch (IOException e) {
            logError("Failed saving catalog file: " + file.getPath(), e);
        }
    }

    private void logError(String message, Exception e) {
        System.err.println("[CatalogManager] " + message);
        e.printStackTrace(System.err);
    }
}
