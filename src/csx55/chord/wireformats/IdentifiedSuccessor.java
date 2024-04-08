package csx55.chord.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IdentifiedSuccessor implements Event {

    private int type;
    private String ipAddress;
    private int port;
    private int purpose;
    private String payload;

    public IdentifiedSuccessor(int type, String ipAddress, int port, int purpose, String payload) {
        this.type = type;
        this.ipAddress = ipAddress;
        this.port = port;
        this.purpose = purpose;
        this.payload = payload;
    }

    public IdentifiedSuccessor(byte[] marshalledData) throws IOException {
        // creating input stream to read byte data sent over network connection
        ByteArrayInputStream inputData = new ByteArrayInputStream(marshalledData);

        // wrap internal bytes array with data input stream
        DataInputStream din = new DataInputStream(new BufferedInputStream(inputData));

        this.type = din.readInt();

        int len = din.readInt();

        byte[] ipData = new byte[len];
        din.readFully(ipData, 0, len);

        this.ipAddress = new String(ipData);

        this.port = din.readInt();

        this.purpose = din.readInt();

        len = din.readInt();
        byte[] data = new byte[len];
        din.readFully(data);
        this.payload = new String(data);

        inputData.close();
        din.close();
    }

    public int getType() {
        return type;
    }

    /**
     * Marshals the Register object into a byte array.
     * 
     * @return The marshalled byte array.
     * @throws IOException If an I/O error occurs.
     */
    public byte[] getBytes() throws IOException {
        byte[] marshalledData;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(outputStream));

        dout.writeInt(type);

        byte[] ipBytes = ipAddress.getBytes();
        dout.writeInt(ipBytes.length);
        dout.write(ipBytes);

        dout.writeInt(port);

        dout.writeInt(purpose);

        byte[] payloadBytes = payload.getBytes();
        dout.writeInt(payloadBytes.length);
        dout.write(payloadBytes);

        dout.flush();
        marshalledData = outputStream.toByteArray();

        outputStream.close();
        dout.close();
        return marshalledData;

    }

    public String getConnectionReadable() {
        return this.ipAddress + ":" + Integer.toString(this.port);
    }

    public String getIPAddress() {
        return ipAddress;
    }

    public int getPort() {
        return port;
    }

    public int getPurpose() {
        return purpose;
    }

}
