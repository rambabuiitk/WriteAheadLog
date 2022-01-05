package wal;

import java.nio.ByteBuffer;

public class WALEntry {
    private Long entryId;
    private final byte[] data;
    private final Integer entryType;
    private long timestamp;

    public WALEntry(Long entryId, byte[] data, Integer entryType) {
        this(entryId, data, entryType, System.currentTimeMillis());
    }

    public WALEntry(Long entryId, byte[] data, Integer entryType, long timeInMillis) {
        this.entryId = entryId;
        this.data = data;
        this.entryType = entryType;
        this.timestamp = timeInMillis;
    }

    public Long getEntryId() {
        return entryId;
    }

    public byte[] getData() {
        return data;
    }

    public Integer getEntryType() {
        return entryType;
    }

    public ByteBuffer serialize() {
        Integer entrySize = entrySize();
        int bufferSize = entrySize + 4; //4 bytes for record length + walEntry size
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        buffer.clear();
        buffer.putInt(entrySize);
        buffer.putInt(entryType); //normal entry
        buffer.putLong(entryId);
        buffer.putLong(timestamp);
        buffer.put(data);
        return buffer;
    }

    Integer entrySize() {
        return data.length + 2 * WriteAheadLog.sizeOfLong + WriteAheadLog.sizeOfInt; //size of all the fields
    }

    public void setEntryId(long entryId) {
        this.entryId = entryId;
    }
}
