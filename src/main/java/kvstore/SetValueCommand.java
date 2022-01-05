package kvstore;

import java.io.*;

public class SetValueCommand implements Command {

    final String key;
    final String value;

    public SetValueCommand(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public byte[] serialize() {
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(os);
            dataOutputStream.writeInt(Command.SetValueType);
            dataOutputStream.writeUTF(key);
            dataOutputStream.writeUTF(value);
            return os.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}

