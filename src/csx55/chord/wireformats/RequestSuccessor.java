package csx55.chord.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Request successor to a random live peer when a node is joining the chord.
 */
public class RequestSuccessor implements Event, Serializable {
    private int type;

    private int lookupKey;

    private String payload;

    private String sourceIP;

    private int sourcePort;

    private int purpose;

    private int hopsCount = 0;

    private List<Integer> hops = new ArrayList<>();

    /* TODO: add address and port of the one that is requesting */

    public RequestSuccessor(int type, int purpose, String payload, int lookupKey, String sourceIP, int sourcePort) {
        this.type = type;
        this.lookupKey = lookupKey;
        this.sourceIP = sourceIP;
        this.sourcePort = sourcePort;
        this.purpose = purpose;
        this.payload = payload;
    }

    public RequestSuccessor(byte[] marshalledData) throws IOException {

        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        // wrap internal bytes array with data input stream
        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        int len = din.readInt();
        byte[] data = new byte[len];
        din.readFully(data);
        this.payload = new String(data);

        this.lookupKey = din.readInt();

        int ipLen = din.readInt();
        byte[] ipData = new byte[ipLen];
        din.readFully(ipData);
        this.sourceIP = new String(ipData);

        this.sourcePort = din.readInt();

        this.purpose = din.readInt();

        this.hopsCount = din.readInt();

        int hopsListLen = din.readInt();
        List<Integer> hopsList = new ArrayList<>(hopsListLen);
        int hop;
        for (int i = 0; i < hopsListLen; i++) {
            hop = din.readInt();
            hopsList.add(hop);
        }

        this.hops = hopsList;

        inputData.close();
        din.close();
    }

    public int getType() {
        return type;
    }

    public byte[] getBytes() throws IOException {
        byte[] marshalledData;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(outputStream));

        dout.writeInt(type);

        byte[] payloadBytes = payload.getBytes();
        dout.writeInt(payloadBytes.length);
        dout.write(payloadBytes);

        dout.writeInt(lookupKey);

        byte[] ipBytes = sourceIP.getBytes();
        dout.writeInt(ipBytes.length);
        dout.write(ipBytes);

        dout.writeInt(sourcePort);

        dout.writeInt(purpose);

        dout.writeInt(hopsCount);

        dout.writeInt(hops.size());

        for (int element : hops) {
            dout.writeInt(element);
        }

        dout.flush();
        marshalledData = outputStream.toByteArray();

        outputStream.close();
        dout.close();
        return marshalledData;

    }

    public String getAddress() {
        return sourceIP;
    }

    public int getPort() {
        return sourcePort;
    }

    public int getLookupKey() {
        return lookupKey;
    }

    public void incrementHops() {
        hopsCount++;
    }

    public int getPurpose() {
        return purpose;
    }

    public String getPayload() {
        return payload;
    }

    public void addPeerToHops(int peerID) {
        hops.add(peerID);
    }

    public List<Integer> getHopsList() {
        return hops;
    }

    public int getHopsCount() {
        return hopsCount;
    }

}