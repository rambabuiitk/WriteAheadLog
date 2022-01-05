package wal;

public class WALConfig {
    private static final long DEFAULT_MAX_LOG_SIZE = 1000;
    static final String DEFAULT_DIR = "/tmp/wal";

    private long maxLogSize = DEFAULT_MAX_LOG_SIZE;
    private String walDir = DEFAULT_DIR;

    public WALConfig() {

    }

    public long getMaxLogSize() {
        return maxLogSize;
    }

    public void setMaxLogSize(long maxLogSize) {
        this.maxLogSize = maxLogSize;
    }

    public String getWalDir() {
        return walDir;
    }

    public void setWalDir(String walDir) {
        this.walDir = walDir;
    }

    public long getCleanTaskIntervalMs() {
        return 0;
    }
}
