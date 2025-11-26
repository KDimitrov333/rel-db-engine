package db.engine.bench;

import db.engine.catalog.CatalogManager;
import db.engine.catalog.ColumnSchema;
import db.engine.catalog.DataType;
import db.engine.catalog.TableSchema;
import db.engine.index.IndexManager;
import db.engine.query.QueryProcessor;
import db.engine.exec.Row;
import db.engine.storage.Record;
import db.engine.storage.StorageManager;

import java.io.File;
import java.util.List;
import java.util.Random;

/**
 * Micro-benchmark harness for the engine.
 * NOT a unit test; run manually. Recommended invocation (uses fat jar with dependencies):
 *
 *   mvn -q -DskipTests package
 *   java -cp target/relational-db-engine-all.jar db.engine.bench.PerfBench
 *
 * To change the number of seeded `students` rows, edit the `DEFAULT_ROWS` constant in
 * this file or pass a numeric CLI argument. Editing `DEFAULT_ROWS` is the simplest
 * way to make a reproducible, checked-in change for benchmarks.
 *
 * The harness prints timings (ms) for these operations:
 *  - Full table scan (SELECT * FROM students)
 *  - Indexed equality lookup (id = mid value)
 *  - Non-index equality lookup (name = 'X...' ) for comparison
 *  - Join (students JOIN enrollments)
 */
public class PerfBench {

    // Change this constant to adjust the default number of rows seeded for the benchmark.
    private static final int DEFAULT_ROWS = 10000;
    // How often to log progress during seeding (in rows)
    private static final int LOG_INTERVAL = 1000;

    public static void main(String[] args) {
        int rows = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_ROWS;
        int enrollRows = Math.max(1, rows / 10); // smaller second table
        System.out.println("-- PerfBench starting: students=" + rows + ", enrollments=" + enrollRows);
        File dataDir = new File("benchdata");
        // Ensure benchdata is clean between runs to avoid accumulating prior inserts
        deleteDir(dataDir);
        dataDir.mkdirs();
        // Clean previous bench files and also clear global catalog/index files so the
        // benchmark gets a fresh CatalogManager (avoids reading tables registered by Main).
        cleanFile("catalog/tables.json");
        cleanFile("catalog/indexes.json");
        // remove any leftover index files under indexes/
        deleteDir(new File("indexes"));

        CatalogManager catalog = new CatalogManager();
        StorageManager storage = new StorageManager(catalog);
        IndexManager index = new IndexManager(catalog, storage);

        TableSchema students = new TableSchema(
            "students",
            List.of(
                new ColumnSchema("id", DataType.INT, 0),
                new ColumnSchema("name", DataType.VARCHAR, 50),
                new ColumnSchema("active", DataType.BOOLEAN, 0)
            ),
            "benchdata/students.tbl"
        );
        storage.createTable(students);

        TableSchema enroll = new TableSchema(
            "enrollments",
            List.of(
                new ColumnSchema("id", DataType.INT, 0),
                new ColumnSchema("student_id", DataType.INT, 0),
                new ColumnSchema("course", DataType.VARCHAR, 50)
            ),
            "benchdata/enrollments.tbl"
        );
        storage.createTable(enroll);

        long overallStart = System.nanoTime();

        long seedStart = System.nanoTime();
        seedStudents(storage, rows);
        long seedStudentsMs = (System.nanoTime() - seedStart) / 1_000_000;

        long enrollStart = System.nanoTime();
        seedEnrollments(storage, enrollRows, rows);
        long seedEnrollMs = (System.nanoTime() - enrollStart) / 1_000_000;

        long idxStart = System.nanoTime();
        index.createIndex("students_id_idx", "students", "id");
        long idxMs = (System.nanoTime() - idxStart) / 1_000_000;

        long seedingTotalMs = seedStudentsMs + seedEnrollMs;
        System.out.println("Seeding times (ms): students=" + seedStudentsMs + " enrollments=" + seedEnrollMs + " indexCreate=" + idxMs);

        QueryProcessor qp = new QueryProcessor(catalog, storage, index);

        // Warm-up (JIT) small queries
        runAndDiscard(qp, "SELECT * FROM students WHERE id = 1");
        runAndDiscard(qp, "SELECT * FROM students JOIN enrollments ON id = student_id WHERE id = 1");

        long scanMs = time(qp, "SELECT * FROM students");
        int mid = rows / 2;
        long eqIndexMs = time(qp, "SELECT * FROM students WHERE id = " + mid);
        // pick a generated name that exists: name pattern is NameXXXX
        long eqScanMs = time(qp, "SELECT * FROM students WHERE name = 'Name" + pad(mid) + "'");
        long joinMs = time(qp, "SELECT * FROM students JOIN enrollments ON id = student_id");

        long queriesTotalMs = scanMs + eqIndexMs + eqScanMs + joinMs;
        long overallMs = (System.nanoTime() - overallStart) / 1_000_000;

        // Print overall breakdown
        System.out.println();
        System.out.println("Overall timing (ms): total=" + overallMs);
        printRow("Seeding Total", seedingTotalMs);
        printRow(" Index Create", idxMs);
        printRow("Query Phase", queriesTotalMs);
        // percentages
        double pctSeed = seedingTotalMs * 100.0 / overallMs;
        double pctIdx = idxMs * 100.0 / overallMs;
        double pctQuery = queriesTotalMs * 100.0 / overallMs;
        System.out.printf("Breakdown: seeding=%.1f%% index=%.1f%% queries=%.1f%%%n", pctSeed, pctIdx, pctQuery);

        System.out.println();
        System.out.println("Results (milliseconds):");
        printRow("Full Scan", scanMs);
        printRow("Indexed Equality", eqIndexMs);
        printRow("Non-index Equality", eqScanMs);
        printRow("Join", joinMs);
        System.out.println("-- PerfBench complete");
    }

    private static void seedStudents(StorageManager storage, int rows) {
        Random rnd = new Random(42);
        for (int i = 1; i <= rows; i++) {
            int id = i;
            String name = "Name" + pad(i);
            boolean active = rnd.nextBoolean();
            storage.insert("students", new Record(List.of(id, name, active)));
            if (i % LOG_INTERVAL == 0) {
                System.out.println("Seeded students: " + i + "/" + rows);
            }
        }
    }

    private static void seedEnrollments(StorageManager storage, int rows, int maxStudentId) {
        Random rnd = new Random(99);
        String[] courses = {"Math","Physics","Chemistry","Biology","History","Art"};
        for (int i = 1; i <= rows; i++) {
            int eid = 100000 + i;
            int sid = 1 + rnd.nextInt(maxStudentId); // uniform distribution
            String course = courses[rnd.nextInt(courses.length)];
            storage.insert("enrollments", new Record(List.of(eid, sid, course)));
            if (i % LOG_INTERVAL == 0) {
                System.out.println("Seeded enrollments: " + i + "/" + rows);
            }
        }
    }

    private static long time(QueryProcessor qp, String sql) {
        long start = System.nanoTime();
        int count = 0;
        for (Row r : qp.execute(sql)) { if (r != null) count++; }
        long end = System.nanoTime();
        long ms = (end - start) / 1_000_000;
        System.out.println(sql + " => rows=" + count + " time=" + ms + "ms");
        return ms;
    }

    private static void runAndDiscard(QueryProcessor qp, String sql) {
        for (Row r : qp.execute(sql)) { if (r == null) break; }
    }

    private static void cleanFile(String path) {
        File f = new File(path);
        if (f.exists()) f.delete();
    }

    private static void deleteDir(File dir) {
        if (!dir.exists()) return;
        File[] children = dir.listFiles();
        if (children != null) {
            for (File c : children) {
                if (c.isDirectory()) deleteDir(c);
                else c.delete();
            }
        }
        dir.delete();
    }

    private static String pad(int i) {
        String s = String.valueOf(i);
        while (s.length() < 5) s = "0" + s; // 5 digits for alignment
        return s;
    }

    private static void printRow(String label, long ms) {
        System.out.printf("%-18s %8d%n", label + ":", ms);
    }
}
