package csx55.chord.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class FileNotFound implements Event {
    private int type;
    private String message;

    public FileNotFound(String message) {
        this.type = Protocol.FILE_NOT_FOUND;
        this.message = message;
    }

    public FileNotFound(byte[] marshalledData) throws IOException {
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);
        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();
        int len = din.readInt();
        byte[] infoData = new byte[len];
        din.readFully(infoData, 0, len);
        this.message = new String(infoData);

        inputData.close();
        din.close();
    }

    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream opStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(opStream));

        dout.writeInt(type);
        byte[] infoData = message.getBytes();
        dout.writeInt(infoData.length);
        dout.write(infoData);

        // making sure data from buffer is flushed
        dout.flush();
        byte[] marshalledData = opStream.toByteArray();

        opStream.close();
        dout.close();
        return marshalledData;
    }

    public int getType() {
        return type;
    }

    public String getMessage() {
        return message;
    }

}
