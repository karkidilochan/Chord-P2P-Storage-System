package csx55.chord.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DownloadResponse implements Event {

    private String fileName;
    private int type;
    private int hopsCount = 0;
    private List<Integer> hops = new ArrayList<>();
    private byte[] payload;

    public DownloadResponse(String fileName, int hopsCount, List<Integer> hops, byte[] payload) {
        this.type = Protocol.DOWNLOAD_RESPONSE;
        this.fileName = fileName;
        this.hopsCount = hopsCount;
        this.hops = hops;
        this.payload = payload;
    }

    public DownloadResponse(byte[] marshalledData) throws IOException {
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        this.hopsCount = din.readInt();

        int hopsListLen = din.readInt();
        List<Integer> hopsList = new ArrayList<>(hopsListLen);
        int hop;
        for (int i = 0; i < hopsListLen; i++) {
            hop = din.readInt();
            hopsList.add(hop);
        }

        this.hops = hopsList;

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

        dout.writeInt(hopsCount);

        dout.writeInt(hops.size());

        for (int element : hops) {
            dout.writeInt(element);
        }

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

    public String getFileName() {
        return fileName;
    }

    public int getType() {
        return type;
    }

    public int getHopsCount() {
        return hopsCount;
    }

    public List<Integer> getHopList() {
        return hops;
    }

    public byte[] getPayload() {
        return payload;
    }
}
