package csx55.chord.utils;

public class Entry {
    /* this is the ring position */
    int start;

    /* ipAddress:port string */
    String ipAddress;

    int port;

    public Entry(int start, String ipAddress, int port) {
        this.start = start;
        this.ipAddress = ipAddress;
        this.port = port;
    }

}
