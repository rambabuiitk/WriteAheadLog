package kvstore;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

public class RemoveValueCommand implements Command {
    private final String key;

    public RemoveValueCommand(String key) {
        this.key = key;
    }

    @Override
    public byte[] serialize() {
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(os);
            dataOutputStream.writeInt(Command.RemoveValueType);
            dataOutputStream.writeUTF(key);
            return os.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
