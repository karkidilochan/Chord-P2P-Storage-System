package csx55.chord.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * Request successor to a random live peer when a node is joining the chord.
 */
public class RequestSuccessor implements Event, Serializable {
    private int type;

    private int peerID;

    private int forwardedCount;

    public RequestSuccessor(int type, int peerID) {
        this.type = type;
        this.peerID = peerID;
    }

    public RequestSuccessor(byte[] marshalledData) throws IOException {

        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        // wrap internal bytes array with data input stream
        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        this.peerID = din.readInt();

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

        dout.writeInt(peerID);

        dout.flush();
        marshalledData = outputStream.toByteArray();

        outputStream.close();
        dout.close();
        return marshalledData;

    }

    public int getPeerID() {
        return peerID;
    }

    public void incrementForwarded() {
        forwardedCount++;
    }

}