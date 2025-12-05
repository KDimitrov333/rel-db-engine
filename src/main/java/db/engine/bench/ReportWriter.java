package db.engine.bench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.LinkedHashMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class ReportWriter {
    private final Path outDir;

    public ReportWriter(Path outDir) {
        this.outDir = outDir;
    }


      // Write a JSON report with per-query stats (milliseconds)
      public void writeJson(Map<String, StatsAggregator> statsPerQuery,
                  Map<String, StatsAggregator> rowsPerQuery,
                  Map<String, String> queryDescriptions,
                  Map<String, String> querySql,
                  BenchmarkConfig cfg) throws IOException {
        if (!Files.exists(outDir)) Files.createDirectories(outDir);
        String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        Path json = outDir.resolve("bench_" + ts + ".json");

        Map<String, Object> root = new LinkedHashMap<>();
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("rows_students", cfg.rowsStudents);
        config.put("rows_enrollments", cfg.rowsEnrollments);
        config.put("id_pool", cfg.idPoolSize);
        config.put("runs", cfg.runs);
        config.put("warmup", cfg.warmup);
        config.put("names", cfg.useNames);
        root.put("config", config);

        Map<String, Object> queries = new LinkedHashMap<>();
        for (Map.Entry<String, StatsAggregator> e : statsPerQuery.entrySet()) {
          String key = e.getKey();
          StatsAggregator s = e.getValue();
          StatsAggregator r = rowsPerQuery.get(key);
          Map<String, Object> entry = new LinkedHashMap<>();
          entry.put("description", queryDescriptions.getOrDefault(key, ""));
          entry.put("sql_template", querySql.getOrDefault(key, ""));
          // Convert timings to milliseconds with fractional precision
          double meanMs = s.mean() / 1_000_000.0;
          double medianMs = s.median() / 1_000_000.0;
          double minMs = s.min() / 1_000_000.0;
          double maxMs = s.max() / 1_000_000.0;
          double stddevMs = s.stddev() / 1_000_000.0;
          double varianceMs2 = s.variance() / (1_000_000.0 * 1_000_000.0);
          // Round to sensible decimals (mean/stddev 2dp, others 3dp)
          entry.put("mean_ms", Math.round(meanMs * 100.0) / 100.0);
          entry.put("median_ms", Math.round(medianMs * 1000.0) / 1000.0);
          entry.put("min_ms", Math.round(minMs * 1000.0) / 1000.0);
          entry.put("max_ms", Math.round(maxMs * 1000.0) / 1000.0);
          entry.put("stddev_ms", Math.round(stddevMs * 100.0) / 100.0);
          entry.put("variance_ms2", Math.round(varianceMs2 * 100.0) / 100.0);
          if (r != null) {
            // Only report distribution metrics for hit queries where variability matters
            if ("equality_hit".equals(key) || "equality_seq".equals(key) || "range".equals(key)) {
              entry.put("rows_median", r.median());
              entry.put("rows_stddev", Math.round(r.stddev()));
            } else {
              // For scan/join/miss, report a single representative rows value (median of samples)
              entry.put("rows", r.median());
            }
          }
          queries.put(key, entry);
        }
        root.put("queries", queries);

        // Disable HTML escaping for readable SQL templates
        Gson gson = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();
        Files.writeString(json, gson.toJson(root));
      }
}
