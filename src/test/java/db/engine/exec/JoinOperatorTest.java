package db.engine.exec;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

import db.engine.catalog.CatalogManager;
import db.engine.catalog.TestCatalogManager;
import db.engine.catalog.ColumnSchema;
import db.engine.catalog.DataType;
import db.engine.catalog.TableSchema;
import db.engine.storage.Record;
import db.engine.storage.StorageManager;

public class JoinOperatorTest {

    private static TableSchema studentsSchema(String suffix) {
        return new TableSchema("students", List.of(
            new ColumnSchema("id", DataType.INT, 0),
            new ColumnSchema("name", DataType.VARCHAR, 50),
            new ColumnSchema("active", DataType.BOOLEAN, 0)
        ), "target/test-students-" + suffix + ".tbl");
    }

    private static TableSchema enrollmentsSchema(String suffix) {
        return new TableSchema("enrollments", List.of(
            new ColumnSchema("id", DataType.INT, 0),
            new ColumnSchema("student_id", DataType.INT, 0),
            new ColumnSchema("course", DataType.VARCHAR, 50)
        ), "target/test-enrollments-" + suffix + ".tbl");
    }

    @Test
    void innerJoinProducesExpectedCombinedRows() {
    String suffix = Long.toString(System.nanoTime());
    CatalogManager catalog = new TestCatalogManager();
    StorageManager storage = new StorageManager(catalog);
    storage.createTable(studentsSchema(suffix));
    storage.createTable(enrollmentsSchema(suffix));

        // seed students (including duplicate id=2)
        List<Record> students = List.of(
            new Record(List.of(1, "Alice", true)),
            new Record(List.of(2, "Bob", false)),
            new Record(List.of(2, "Bobby", true)),
            new Record(List.of(3, "Eve", true))
        );
        for (Record r : students) storage.insert("students", r);

        // seed enrollments
        List<Record> enrolls = List.of(
            new Record(List.of(100, 1, "Math")),
            new Record(List.of(101, 1, "Physics")),
            new Record(List.of(102, 2, "Chemistry")),
            new Record(List.of(103, 2, "Biology")),
            new Record(List.of(104, 3, "Math"))
        );
        for (Record r : enrolls) storage.insert("enrollments", r);

        // Build scan operators (wrapped with schema by planner normally)
        Operator left = new SeqScanOperator(storage, "students");
        Operator right = new SeqScanOperator(storage, "enrollments");
        JoinOperator join = new JoinOperator(leftWithSchema(left), rightWithSchema(right), "id", "student_id");

        join.open();
        int count = 0;
        Row row;
        while ((row = join.next()) != null) {
            count++;
            List<Object> v = row.values();
            // Basic sanity: left part has 3 cols, right part has 3 cols => 6 total
            assertEquals(6, v.size());
        }
        join.close();
        // Expected matches: id=1 has 2, id=2 has 2 (duplicated student generates 4), id=3 has 1 => 7 rows
        assertEquals(7, count);
    }

    private Operator leftWithSchema(Operator op) {
        return new Operator() {
            @Override public void open() { op.open(); }
            @Override public Row next() { return op.next(); }
            @Override public void close() { op.close(); }
            @Override public List<ColumnSchema> schema() { return List.of(
                new ColumnSchema("id", DataType.INT, 0),
                new ColumnSchema("name", DataType.VARCHAR, 50),
                new ColumnSchema("active", DataType.BOOLEAN, 0)
            ); }
        };
    }

    private Operator rightWithSchema(Operator op) {
        return new Operator() {
            @Override public void open() { op.open(); }
            @Override public Row next() { return op.next(); }
            @Override public void close() { op.close(); }
            @Override public List<ColumnSchema> schema() { return List.of(
                new ColumnSchema("id", DataType.INT, 0),
                new ColumnSchema("student_id", DataType.INT, 0),
                new ColumnSchema("course", DataType.VARCHAR, 50)
            ); }
        };
    }
}
