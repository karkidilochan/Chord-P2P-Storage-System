package csx55.wireformats;

/**
 * Protocol interface defines the protocol constants used for communication
 * between nodes.
 */
public interface Protocol {

    final int REGISTER_REQUEST = 0;
    final int REGISTER_RESPONSE = 1;
    final int DEREGISTER_REQUEST = 2;
    final int MESSAGING_NODES_LIST = 3;
    final int SUCCESSOR_IDENTIFIED = 4;
    final int FILE_TRANSFER = 5;

    final int NOTIFY_SUCCESSOR = 6;
    final int NOTIFY_PREDECESSOR = 7;

    final int GET_PREDECESSOR = 8;
    final int GET_PREDECESSOR_RESPONSE = 9;

    final int FILE_TRANSFER_RESPONSE = 10;
    final int DOWNLOAD_REQUEST = 11;
    final int FILE_NOT_FOUND = 12;
    final int DOWNLOAD_RESPONSE = 13;
    final int COLLISION = 14;
    final int REQUEST_SUCCESSOR = 15;

    final int SETUP_CHORD = 16;

    final byte SUCCESS = (byte) 200;
    final byte FAILURE = (byte) 500;
}
