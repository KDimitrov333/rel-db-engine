package db.engine.bench;

import java.util.concurrent.ThreadLocalRandom;

public class DataPool {
    private final int[] ids;

    public DataPool(int poolSize, long seed) {
        this.ids = new int[poolSize];
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        // Initialize IDs from a wide range to avoid trivial sequences
        for (int i = 0; i < poolSize; i++) {
            // Approx. 1..1e9 spread; duplicates possible by sampling
            ids[i] = 1 + rnd.nextInt(1_000_000_000);
        }
    }

    public int randomId() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        return ids[rnd.nextInt(ids.length)];
    }
}
