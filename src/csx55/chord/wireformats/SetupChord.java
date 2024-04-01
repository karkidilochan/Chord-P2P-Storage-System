package csx55.chord.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Register class represents a message for registering or deregistering a node.
 */
public class SetupChord implements Event, Serializable {
    private int type;

    private String ipAddress;
    private int port;
    private String hostName;

    /**
     * Constructs a Register object for registering or deregistering a node.
     * 
     * @param type      The type of registration.
     * @param ipAddress The IP address of the node.
     * @param port      The port number of the node.
     */
    public SetupChord(int type, String ipAddress, int port) {
        this.type = type;
        this.ipAddress = ipAddress;
        this.port = port;
    }

    /**
     * Constructs a Register object by unmarshalling the byte array.
     * 
     * @param marshalledData The marshalled byte array containing the data.
     */
    public SetupChord(byte[] marshalledData) throws IOException, ClassNotFoundException {
        // creating input stream to read byte data sent over network connection
        ByteArrayInputStream bis = new ByteArrayInputStream(marshalledData);
        ObjectInputStream in = new ObjectInputStream(bis);
        SetupChord newObject = (SetupChord) in.readObject();
        copyObject(newObject);
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
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos);
        out.writeObject(this);
        marshalledData = bos.toByteArray();
        return marshalledData;

    }

    private void copyObject(SetupChord newObject) {
        this.ipAddress = newObject.ipAddress;
        this.port = newObject.port;
        this.hostName = newObject.hostName;
    }

    /**
     * Returns a readable representation of the connection details.
     * 
     * @return A string representing the IP address and port.
     */
    public String getConnectionReadable() {
        return this.ipAddress + ":" + Integer.toString(this.port);
    }

    public String getIPAddress() {
        return ipAddress;
    }

    public int getPort() {
        return port;
    }

}