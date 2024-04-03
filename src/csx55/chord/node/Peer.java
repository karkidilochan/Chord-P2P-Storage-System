package csx55.chord.node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import csx55.chord.tcp.TCPConnection;
import csx55.chord.tcp.TCPServer;
import csx55.chord.wireformats.Event;
import csx55.chord.wireformats.GetPredecessor;
import csx55.chord.wireformats.GetPredecessorResponse;
import csx55.chord.wireformats.IdentifiedSuccessor;
import csx55.chord.wireformats.NotifyPredecessor;
import csx55.chord.wireformats.NotifySuccessor;
import csx55.chord.wireformats.Protocol;
import csx55.chord.wireformats.Register;
import csx55.chord.wireformats.RegisterResponse;
import csx55.chord.wireformats.RequestSuccessor;
import csx55.chord.wireformats.SetupChord;

/**
 * Implementation of the Node interface, represents a messaging node in the
 * network overlay system.
 * Messaging nodes facilitate communication between other nodes in the overlay.
 * This class handles registration with a registry, establishment of
 * connections,
 * message routing, and messageStatistics tracking.
 */
public class Peer implements Node, Protocol {

    /*
     * port to listen for incoming connections, configured during messaging node
     * creation
     */
    private final Integer nodePort;
    private final String hostName;
    private final Integer peerID;
    private final String hostIP;
    private final String fullAddress;

    private FingerTable fingerTable;

    private String successor;
    private String predecessor;

    // Constants for command strings

    // create a TCP connection with the Registry
    private TCPConnection registryConnection;

    private TCPConnection predecessorConnection;

    private TCPConnection successorConnection;

    private final int FT_ROWS = 32;

    private Peer(String hostName, String hostIP, int nodePort, int peerID) {
        this.hostName = hostName;
        this.hostIP = hostIP;
        this.nodePort = nodePort;
        this.peerID = peerID;
        this.fullAddress = hostIP + ":" + nodePort;
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            printUsageAndExit();
        }
        System.out.println("Messaging node live at: " + new Date());
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            // assign a random available port
            int nodePort = serverSocket.getLocalPort();

            String hostIP = InetAddress.getLocalHost().getHostAddress();

            /* 32-bit integer id for peer */
            String hostString = hostIP + ":" + String.valueOf(nodePort);
            int peerID = hostString.hashCode();

            /*
             * get local host name and use assigned nodePort to initialize a messaging node
             */
            Peer node = new Peer(
                    InetAddress.getLocalHost().getHostName(), hostIP, nodePort, peerID);

            /* start a new TCP server thread */
            (new Thread(new TCPServer(node, serverSocket))).start();

            // register this node with the registry
            node.registerNode(args[0], Integer.valueOf(args[1]));

            // facilitate user input in the console
            node.takeCommands();
        } catch (IOException e) {
            System.out.println("An error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Print the correct usage of the program and exits with a non-zero status
     * code.
     */
    private static void printUsageAndExit() {
        System.err.println("Usage: java csx55.chord.node.Peer discovery-ip discovery-port");
        System.exit(1);
    }

    /**
     * Registers this node with the registry.
     *
     * @param registryHost The host name of the registry.
     * @param registryPort The port on which the registry listens for connections.
     */
    private void registerNode(String registryHost, Integer registryPort) {
        try {
            // create a socket to the Registry server
            Socket socketToRegistry = new Socket(registryHost, registryPort);
            TCPConnection connection = new TCPConnection(this, socketToRegistry);

            Register register = new Register(Protocol.REGISTER_REQUEST,
                    this.hostIP, this.nodePort, this.hostName, this.peerID);

            System.out.println(
                    "Address of the peer node is: " + this.hostIP + ":" + this.nodePort);

            // send "Register" message to the Registry
            connection.getTCPSenderThread().sendData(register.getBytes());
            connection.start();

            // Set the registry connection for this node
            this.registryConnection = connection;

            fingerTable = new FingerTable(peerID);

            /* initialize finger table */
            for (int i = 1; i <= FT_ROWS; i++) {
                fingerTable.getFingerTable().put(i, null);
            }
        } catch (IOException | InterruptedException e) {
            System.out.println("Error registering node: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Handle user interaction with the messaging node.
     * Allows users to input commands for interacting with processes.
     * Commands include print-shortest-path and exit-overlay.
     */
    private void takeCommands() {
        System.out.println(
                "Enter a command to interact with the messaging node. Available commands: print-shortest-path, exit-overlay\n");
        boolean stop = false;
        try (Scanner scan = new Scanner(System.in)) {
            while (!stop) {
                String command = scan.nextLine().toLowerCase();
                switch (command) {
                    case "exit-overlay":
                        exitOverlay();

                    default:
                        System.out.println("Invalid Command. Available commands: print-shortest-path, exit-overlay\\n");
                        break;
                }
            }
        } catch (Exception e) {
            System.err.println("An error occurred during command processing: " + e.getMessage());
            e.printStackTrace();
        } finally {
            System.out.println("Deregistering the messaging node and terminating: " + hostName + ":" + nodePort);
            exitOverlay();
            System.exit(0);
        }
    }

    /**
     * Handles incoming messages from the TCP connection.
     *
     * @param event      The event to handle.
     * @param connection The TCP connection associated with the event.
     */
    public void handleIncomingEvent(Event event, TCPConnection connection) {
        System.out.println("Received event: " + event.toString());

        switch (event.getType()) {

            case Protocol.REGISTER_RESPONSE:
                handleRegisterResponse((RegisterResponse) event);
                break;

            case Protocol.SETUP_CHORD:
                handleDiscoveryMessage((SetupChord) event);
                break;

            case Protocol.REQUEST_SUCCESSOR:
                handleSuccessorRequest((RequestSuccessor) event, connection);
                break;

            case Protocol.SUCCESSOR_IDENTIFIED:
                handleSuccessorResponse((IdentifiedSuccessor) event);
                break;

            case Protocol.GET_PREDECESSOR:
                sendPredecessor((GetPredecessor) event, connection);
                break;

            case Protocol.GET_PREDECESSOR_RESPONSE:
                receivePredecessor((GetPredecessorResponse) event, connection);
                break;

            case Protocol.NOTIFY_SUCCESSOR:
                updatePredecessor((NotifySuccessor) event);
                break;

            case Protocol.NOTIFY_PREDECESSOR:
                updateSuccessor((NotifyPredecessor) event);
                break;

        }
    }

    /**
     * Handles a REGISTER_RESPONSE event.
     *
     * @param response The REGISTER_RESPONSE event to handle.
     */
    private void handleRegisterResponse(RegisterResponse response) {
        System.out.println("Received registration response from the registry: " + response.toString());
    }

    /**
     * Creates overlay connections with the messaging nodes listed in the
     * provided MessagingNodeList.
     *
     * @param nodeList The MessagingNodesList containing information about the
     *                 peers.
     */

    private void handleDiscoveryMessage(SetupChord message) {

        System.out.println(fullAddress);
        System.out.println(message.getConnectionReadable());
        System.out.println(message.getConnectionReadable() == fullAddress);
        /*
         * first check if the setup chord is self or not i.e. its the first peer in the
         * chord
         */
        if (message.getConnectionReadable().equals(fullAddress)) {
            /* setup finger table by adding self to the finger table rows */
            this.successor = fullAddress;
            this.predecessor = fullAddress;
            for (int i = 0; i < FT_ROWS; i++) {
                fingerTable.getFingerTable().put(i, fullAddress);
            }
            /* TODO: setup predecessor and successor as self */
        } else {
            try {
                Socket socketToPeer = new Socket(message.getIPAddress(), message.getPort());
                TCPConnection connection = new TCPConnection(this, socketToPeer);

                /* send lookup request of the peer-id to this random peer */

                RequestSuccessor request = new RequestSuccessor(Protocol.REQUEST_SUCCESSOR, this.peerID);

                connection.getTCPSenderThread().sendData(request.getBytes());
                connection.start();

                /* TODO: also initiate the finger table and the predecessor */
            } catch (IOException | InterruptedException e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
        }

    }

    private void exitOverlay() {
        Register register = new Register(Protocol.DEREGISTER_REQUEST,
                this.hostIP, this.nodePort, this.hostName, this.peerID);

        try {
            registryConnection.getTCPSenderThread().sendData(register.getBytes());
            registryConnection.close();
        } catch (IOException | InterruptedException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    private void handleSuccessorRequest(RequestSuccessor message, TCPConnection peerConnection) {
        /* TODO: make sure the finger table is initialized */

        /* perform lookup for the given peer id and send it as response */
        int lookupId = message.getPeerID();

        /*
         * first look for the successor of lookup id in peer table
         * if found, send it back to the original peer
         * else
         * forward the successor request to node with closes succeeding id
         */
        String firstEntry = fingerTable.getFingerTable().get(1);
        try {
            if (lookupId == this.peerID || fingerTable.isSuccessor(firstEntry.hashCode(), lookupId)) {
                /* basically sending this node's successor's network info */
                IdentifiedSuccessor response = new IdentifiedSuccessor(Protocol.SUCCESSOR_IDENTIFIED,
                        firstEntry.split(":")[0], Integer.parseInt(firstEntry.split(":")[1]));

                peerConnection.getTCPSenderThread().sendData(response.getBytes());

            } else {

                /*
                 * now forward the find successor request to closest succeeding id
                 * for this find the closest predecessor and ping to get its successor
                 */
                for (int i = 2; i < FT_ROWS; i++) {
                    String entry = fingerTable.getFingerTable().get(i);
                    if (fingerTable.isSuccessor(entry.hashCode(), lookupId)) {
                        String predecessor = fingerTable.getFingerTable().get(i - 1);
                        /* get its successor */
                        Socket socketToPred = new Socket(predecessor.split(":")[0],
                                Integer.parseInt(predecessor.split(":")[1]));
                        TCPConnection connectionToPred = new TCPConnection(this, socketToPred);

                        connectionToPred.getTCPSenderThread().sendData(message.getBytes());
                        connectionToPred.start();

                    }
                }

            }
            peerConnection.close();

        } catch (IOException | InterruptedException e) {
            System.out.println(
                    "Error occured while forwarding request for successor." + "Forward count is " + e.getMessage());
            e.printStackTrace();

        }

    }

    private void handleSuccessorResponse(IdentifiedSuccessor message) {
        // Get Identifier of successor peer
        this.successor = message.getConnectionReadable();

        // update finger table using this successor
        fingerTable.getFingerTable().put(1, successor);

        // query pred of this successor and make it your predecessor
        // also notify your successor you are its pred

        try {
            Socket socketToSuccessor = new Socket(message.getIPAddress(), message.getPort());
            TCPConnection successorConnection = new TCPConnection(this, socketToSuccessor);

            GetPredecessor request = new GetPredecessor(Protocol.GET_PREDECESSOR, this.hostIP, this.nodePort);

            successorConnection.getTCPSenderThread().sendData(request.getBytes());
            successorConnection.start();
        } catch (IOException | InterruptedException e) {
            System.out.println("Error occurred while requesting predecessor info." + e.getMessage());
            e.printStackTrace();
        }

    }

    private void sendPredecessor(GetPredecessor message, TCPConnection connection) {
        try {
            GetPredecessorResponse response = new GetPredecessorResponse(Protocol.GET_PREDECESSOR_RESPONSE,
                    this.predecessor.split(":")[0],
                    Integer.parseInt(predecessor.split(":")[1]));

            connection.getTCPSenderThread().sendData(response.getBytes());
        } catch (IOException | InterruptedException e) {
            System.out.println("Error occurred while sending predecessor." + e.getMessage());
            e.printStackTrace();
        }

    }

    private void receivePredecessor(GetPredecessorResponse message, TCPConnection connection) {
        this.predecessor = message.getConnectionReadable();

        /*
         * after getting predecessor, notify your successor that you are the new
         * predecessor
         */
        try {
            /* also notify this received predecessor that you are the new successor */
            NotifyPredecessor request = new NotifyPredecessor(Protocol.NOTIFY_PREDECESSOR,
                    this.predecessor.split(":")[0],
                    Integer.parseInt(predecessor.split(":")[1]));

            NotifySuccessor notify = new NotifySuccessor(Protocol.NOTIFY_SUCCESSOR, this.hostIP, this.nodePort);

            connection.getTCPSenderThread().sendData(request.getBytes());
            connection.getTCPSenderThread().sendData(notify.getBytes());
        } catch (IOException | InterruptedException e) {
            System.out.println("Error occurred while notifying successor about new predecessor." + e.getMessage());
            e.printStackTrace();
        }
    }

    private void updatePredecessor(NotifySuccessor message) {
        this.predecessor = message.getConnectionReadable();
        System.out.println("Successfully updated predecessor.");
    }

    private void updateSuccessor(NotifyPredecessor message) {
        this.successor = message.getConnectionReadable();
        fingerTable.getFingerTable().put(1, successor);
        System.out.println("Successfully updated self successor with notification from true successor");

    }

}
