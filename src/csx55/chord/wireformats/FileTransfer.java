package csx55.chord.wireformats;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class FileTransfer {

    private String fileName;
    private String filePath;
    private int type;

    public FileTransfer(int type, String fileName, String filePath) {
        this.type = type;
        this.fileName = fileName;
        this.filePath = filePath;

    }

    public FileTransfer(byte[] marshalledData) {

    }

    public byte[] getBytes() throws IOException {
        byte[] marshalledData;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(outputStream));

        File uploadFile = new File(this.filePath);

    }

}
