package distributed.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DownloadRequest implements Event {

    private String fileName;
    private int type;
    private int hopsCount = 0;
    private List<Integer> hops = new ArrayList<>();

    public DownloadRequest(String fileName, int hopsCount, List<Integer> hops) {
        this.type = Protocol.DOWNLOAD_REQUEST;
        this.fileName = fileName;
        this.hopsCount = hopsCount;
        this.hops = hops;
    }

    public DownloadRequest(byte[] marshalledData) throws IOException {
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
}
