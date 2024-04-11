package csx55.chord.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class FileTransfer implements Event {

    private String fileName;
    private int type;
    private byte[] payload;
    private boolean isUpload;

    public FileTransfer(int type, boolean isUpload, String fileName, byte[] payload) {
        this.type = type;
        this.fileName = fileName;
        this.payload = payload;
        this.isUpload = isUpload;
    }

    public FileTransfer(byte[] marshalledData) throws IOException {
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        this.isUpload = din.readBoolean();

        int len = din.readInt();
        byte[] data = new byte[len];
        din.readFully(data);
        this.fileName = new String(data);

        len = din.readInt();
        byte[] payloadData = new byte[len];
        din.readFully(payloadData);
        this.payload = payloadData;

        inputData.close();
        din.close();

    }

    public byte[] getBytes() throws IOException {
        byte[] marshalledData;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(outputStream));

        dout.writeInt(type);

        dout.writeBoolean(isUpload);

        dout.writeInt(fileName.getBytes().length);
        dout.write(fileName.getBytes());

        dout.writeInt(this.payload.length);
        dout.write(this.payload);

        dout.flush();
        marshalledData = outputStream.toByteArray();

        outputStream.close();
        dout.close();

        return marshalledData;
    }

    public byte[] getFilePayload() {
        return payload;
    }

    public String getFileName() {
        return fileName;
    }

    public boolean checkIfUpload() {
        return isUpload;
    }

    public int getType() {
        return type;
    }

}
