package wal;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class WALEntryDeserializer {
    final ByteBuffer intBuffer = ByteBuffer.allocate(WriteAheadLog.sizeOfInt);
    final ByteBuffer longBuffer = ByteBuffer.allocate(WriteAheadLog.sizeOfLong);
    private FileChannel logChannel;

    public WALEntryDeserializer(FileChannel logChannel) {
        this.logChannel = logChannel;
    }

    WALEntry readEntry() {
        Integer entrySize = readInteger();
        Integer entryType = readInteger();
        Long entryId = readLong();
        Long time = readLong();

        int dataSize = (entrySize - (WriteAheadLog.sizeOfInt + WriteAheadLog.sizeOfLong + WriteAheadLog.sizeOfLong));
        ByteBuffer buffer = ByteBuffer.allocate(dataSize);
        long position = readFromChannel(logChannel, buffer);
        int bytesRead = entrySize + WriteAheadLog.sizeOfInt;
        return new WALEntry(entryId, buffer.array(), entryType);
    }

    public Long readLong() {
        long position = readFromChannel(logChannel, longBuffer);
        return longBuffer.getLong();
    }

    public Integer readInteger() {
        long position = readFromChannel(logChannel, intBuffer);
        return intBuffer.getInt();
    }

    private long readFromChannel(FileChannel channel, ByteBuffer buffer) {

        try {
            buffer.clear();//clear to start reading.

            int bytesRead;
            long currentPosition = channel.position();
            do {
                bytesRead = channel.read(buffer, currentPosition);
                currentPosition += bytesRead;
            } while (bytesRead != -1 && buffer.hasRemaining());

            buffer.flip(); //read to be read

            channel.position(currentPosition); //advance channel position
            return channel.position();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

