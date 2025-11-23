package db.engine.exec;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.List;
import org.junit.jupiter.api.Test;

import db.engine.catalog.ColumnSchema;
import db.engine.catalog.DataType;
import db.engine.catalog.TableSchema;
import db.engine.catalog.TestCatalogManager;
import db.engine.query.Condition;
import db.engine.query.SelectQuery;
import db.engine.query.WhereClause;
import db.engine.query.PredicateCompiler;
import db.engine.storage.Record;
import db.engine.storage.StorageManager;

public class FilterOperatorTest {
    @Test
    void filtersRowsByPredicate() {
        TestCatalogManager catalog = new TestCatalogManager();
        StorageManager storage = new StorageManager(catalog);
        TableSchema ts = new TableSchema("filter_students", List.of(
            new ColumnSchema("id", DataType.INT, 0),
            new ColumnSchema("name", DataType.VARCHAR, 50),
            new ColumnSchema("active", DataType.BOOLEAN, 0)
        ), "target/filter_students.tbl");
        new File(ts.filePath()).delete();
        storage.createTable(ts);
        storage.insert("filter_students", new Record(List.of(1, "A", true)));
        storage.insert("filter_students", new Record(List.of(2, "B", false)));
        storage.insert("filter_students", new Record(List.of(3, "C", true)));
        storage.insert("filter_students", new Record(List.of(4, "D", false)));

        // WHERE active = true OR id < 2
        WhereClause wc = new WhereClause(List.of(
            new Condition("active", Condition.Op.EQ, true, false),
            new Condition("id", Condition.Op.LT, 2, false)
        ), List.of("OR"));
        PredicateCompiler pc = new PredicateCompiler();
        SelectQuery sq = new SelectQuery("filter_students", List.of(), wc, null);
        Predicate pred = pc.compile(sq, ts.columns());

        SeqScanOperator scan = new SeqScanOperator(storage, "filter_students");
        Operator root = new FilterOperator(scan, pred);
        root.open();
    int count = 0;
    while (root.next() != null) count++;
        root.close();
        // Matches rows with (active = true) OR (id < 2).
        // Data: (1,A,true) matches both predicates (count once), (2,B,false) no, (3,C,true) matches active, (4,D,false) no.
        // Total distinct matching rows = 2.
        assertEquals(2, count);
    }
}
