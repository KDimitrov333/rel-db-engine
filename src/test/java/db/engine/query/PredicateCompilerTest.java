package db.engine.query;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

import db.engine.catalog.ColumnSchema;
import db.engine.catalog.DataType;
import db.engine.exec.Predicate;
import db.engine.exec.Row;
import db.engine.storage.Record;

public class PredicateCompilerTest {

    private List<ColumnSchema> schema() {
        return List.of(
            new ColumnSchema("id", DataType.INT, 0),
            new ColumnSchema("name", DataType.VARCHAR, 50),
            new ColumnSchema("active", DataType.BOOLEAN, 0)
        );
    }

    private Row row(Object... vals) {
        return Row.of(new Record(List.of(vals)), null, schema());
    }

    @Test
    void simpleAndOrNotChains() {
        // WHERE active = true AND id >= 5 AND id <= 8
        WhereClause wcRange = new WhereClause(List.of(
            new Condition("active", Condition.Op.EQ, true, false),
            new Condition("id", Condition.Op.GTE, 5, false),
            new Condition("id", Condition.Op.LTE, 8, false)
        ), List.of("AND", "AND"));
        SelectQuery sqRange = new SelectQuery("students", List.of(), wcRange, null);
        PredicateCompiler pc = new PredicateCompiler();
        Predicate predRange = pc.compile(sqRange, schema());
        assertTrue(predRange.test(row(5, "X", true)));
        assertTrue(predRange.test(row(8, "X", true)));
        assertFalse(predRange.test(row(9, "X", true)));
        assertFalse(predRange.test(row(6, "X", false)));

        // WHERE NOT active OR id < 3
        WhereClause wcNotOr = new WhereClause(List.of(
            new Condition("active", Condition.Op.EQ, true, true), // NOT active
            new Condition("id", Condition.Op.LT, 3, false)
        ), List.of("OR"));
        SelectQuery sqNotOr = new SelectQuery("students", List.of(), wcNotOr, null);
        Predicate predNotOr = pc.compile(sqNotOr, schema());
        assertTrue(predNotOr.test(row(1, "A", true))); // id < 3 matches second condition
        assertTrue(predNotOr.test(row(4, "B", false))); // NOT active matches first condition
        assertFalse(predNotOr.test(row(4, "B", true))); // active true and id>=3 fails both
    }
}
