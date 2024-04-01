package csx55.chord.node;

import csx55.chord.tcp.TCPConnection;
import csx55.chord.wireformats.Event;

/**
 * Represents a node capable of handling incoming events from a TCP connection.
 */
public interface Node {
    /**
     * Handles incoming events from the TCP connection.
     *
     * @param event      The event to handle.
     * @param connection The TCP connection associated with the event.
     */
    // implicitly public
    void handleIncomingEvent(Event event, TCPConnection connection);
}
