package csx55.chord.node;

import java.io.File;
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
import csx55.chord.utils.Entry;
import csx55.chord.wireformats.DownloadRequest;
import csx55.chord.wireformats.Event;
import csx55.chord.wireformats.FileTransfer;
import csx55.chord.wireformats.FileTransferResponse;
import csx55.chord.wireformats.FindSuccessorTypes;
import csx55.chord.wireformats.GetPredecessor;
import csx55.chord.wireformats.GetPredecessorResponse;
import csx55.chord.wireformats.IdentifiedSuccessor;
import csx55.chord.wireformats.NotifyYourPredecessor;
import csx55.chord.wireformats.NotifyYourSuccessor;
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

    private final int FT_ROWS = 32;

    // Constants for command strings

    // create a TCP connection with the Registry
    private TCPConnection registryConnection;

    private TCPConnection predecessorConnection;

    private TCPConnection successorConnection;

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
            int peerID = Math.abs(hostString.hashCode());
            System.out.println("hashcode of " + hostString + " " + peerID);

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

            fingerTable = new FingerTable(this.hostIP, this.nodePort, peerID);

            /* initialize finger table */
            fingerTable.initialize();

        } catch (IOException | InterruptedException e) {
            System.out.println("Error registering node: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void takeCommands() {
        System.out.println(
                "Enter a command. Available commands: print-shortest-path, exit-overlay\n");
        try (Scanner scan = new Scanner(System.in)) {
            while (true) {
                String line = scan.nextLine().toLowerCase();
                String[] input = line.split("\\s+");
                switch (input[0]) {

                    case "neighbors":
                        printNeighbors();
                        break;

                    case "files":
                        // TODO:
                        printFiles();
                        break;

                    case "finger-table":
                        fingerTable.print();
                        break;

                    case "upload":
                        PeerUtilities.handleFileUpload(input[1], this, this.fingerTable);
                        break;

                    case "download":
                        PeerUtilities.handleFileDownload(input[1], this, this.fingerTable);
                        break;

                    case "exit":
                        // TODO:
                        exitOverlay();
                        break;

                    default:
                        System.out.println("Invalid Command. Available commands: exit\\n");
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
                updatePredecessor((NotifyYourSuccessor) event);
                break;

            case Protocol.NOTIFY_PREDECESSOR:
                updateSuccessor((NotifyYourPredecessor) event);
                break;

            case Protocol.FILE_TRANSFER:
                PeerUtilities.sendFileReceived((FileTransfer) event, connection, this, fingerTable);
                break;

            case Protocol.FILE_TRANSFER_RESPONSE:
                PeerUtilities.handleFileTransferResponse((FileTransferResponse) event);
                break;

            case Protocol.DOWNLOAD_REQUEST:
                PeerUtilities.handleDownloadRequest((DownloadRequest) event, this, connection);
                break;

        }
    }

    private void handleRegisterResponse(RegisterResponse response) {
        System.out.println("Received registration response from the discovery: " + response.toString());
    }

    private void handleDiscoveryMessage(SetupChord message) {
        /*
         * first check if the setup chord is self or not i.e. its the first peer in the
         * chord
         */
        if (!message.getConnectionReadable().equals(fullAddress)) {
            try {
                Socket socketToPeer = new Socket(message.getIPAddress(), message.getPort());
                TCPConnection connection = new TCPConnection(this, socketToPeer);

                /* send lookup request of the peer-id to this random peer */

                /*
                 * Basic workflow:
                 * 1. send request successor to random node
                 * 2. random node searches for your immediate predecessor, pings it through
                 * forwarded requests i.e. perform lookup hops with requests
                 * 3. the predecessor pings you with its successor
                 * 4. you update your successor with this info
                 */

                RequestSuccessor request = new RequestSuccessor(FindSuccessorTypes.JOIN_REQUEST, fullAddress,
                        this.peerID, this.hostIP, this.nodePort);
                request.addPeerToHops(this.peerID);

                connection.getTCPSenderThread().sendData(request.getBytes());
                connection.start();

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

        /*
         * TODO:
         * while exiting, send all the files you were responsible for to your successor
         */

    }

    private void handleSuccessorRequest(RequestSuccessor message, TCPConnection peerConnection) {
        /* TODO: make sure the finger table is initialized */

        /* perform lookup for the given peer id and send it as response */
        int lookupId = message.getLookupKey();
        message.addPeerToHops(this.peerID);
        message.incrementHops();

        /*
         * first look for the successor of lookup id in peer table
         * if found, send it back to the original peer
         * else
         * forward the successor request to node with closes succeeding id
         */
        try {
            /*
             * key > predecessor
             * key <= self
             */
            if (fingerTable.isWithinRing(lookupId, this.fingerTable.getPredecessor().getHashCode(), this.peerID)) {
                /* basically sending this node's network info */
                IdentifiedSuccessor response = new IdentifiedSuccessor(this.hostIP, this.nodePort, message.getPurpose(),
                        message.getPayload(), message.getHopsCount(),
                        message.getHopsList());

                Socket socketToSource = new Socket(message.getAddress(),
                        message.getPort());
                TCPConnection connectionToSource = new TCPConnection(this, socketToSource);

                connectionToSource.getTCPSenderThread().sendData(response.getBytes());
                connectionToSource.start();

            } else {

                /*
                 * now forward the find successor request to closest succeeding id
                 * for this find the closest predecessor and ping to get its successor
                 */

                // call fingertable lookup function to find the predecessor
                Entry lookupResult = fingerTable.lookup(lookupId);

                /* get its successor */
                Socket socketToPred = new Socket(lookupResult.getAddress(),
                        lookupResult.getPort());
                TCPConnection connectionToPred = new TCPConnection(this, socketToPred);

                connectionToPred.getTCPSenderThread().sendData(message.getBytes());
                connectionToPred.start();

            }
            // peerConnection.close();

        } catch (IOException | InterruptedException e) {
            System.out.println(
                    "Error occured while forwarding request for successor." + "Forward count is " + e.getMessage());
            e.printStackTrace();

        }

    }

    private void handleSuccessorResponse(IdentifiedSuccessor message) {
        int purpose = message.getPurpose();

        switch (purpose) {
            case FindSuccessorTypes.JOIN_REQUEST:
                PeerUtilities.joinNetwork(fingerTable, message, this);
                break;

            case FindSuccessorTypes.FILE_UPLOAD:
                PeerUtilities.sendFileToPeer(message, this);
                break;

            case FindSuccessorTypes.FILE_DOWNLOAD:
                PeerUtilities.sendDownloadRequest(message, this);
                break;

            default:
                break;
        }

    }

    private void sendPredecessor(GetPredecessor message, TCPConnection connection) {
        try {
            GetPredecessorResponse response = new GetPredecessorResponse(
                    fingerTable.getPredecessor().getAddress(),
                    fingerTable.getPredecessor().getPort());

            connection.getTCPSenderThread().sendData(response.getBytes());
        } catch (IOException | InterruptedException e) {
            System.out.println("Error occurred while sending predecessor." + e.getMessage());
            e.printStackTrace();
        }

    }

    private void receivePredecessor(GetPredecessorResponse message, TCPConnection connection) {

        /*
         * TODO: check if this predecessor response has node values that is itself
         * 
         */

        fingerTable.updatePredecessor(message.getIPAddress(), message.getPort());
        ;

        /*
         * after getting predecessor, notify your successor that you are the new
         * predecessor
         */
        try {
            /* also notify this received predecessor that you are the new successor */
            NotifyYourPredecessor request = new NotifyYourPredecessor(
                    this.hostIP, this.nodePort);

            /* notify your successor that you are their new predecessor */
            NotifyYourSuccessor notify = new NotifyYourSuccessor(this.hostIP, this.nodePort);

            connection.getTCPSenderThread().sendData(request.getBytes());
            connection.getTCPSenderThread().sendData(notify.getBytes());

        } catch (IOException | InterruptedException e) {
            System.out.println("Error occurred while notifying successor about new predecessor." + e.getMessage());
            e.printStackTrace();
        }
    }

    private void updatePredecessor(NotifyYourSuccessor message) {
        fingerTable.updatePredecessor(message.getIPAddress(), message.getPort());

        // TODO: now the successor will migrate files to its new predecessor
        // perform files migration process

        System.out.println("Successfully updated predecessor.");
    }

    private void updateSuccessor(NotifyYourPredecessor message) {
        fingerTable.updateSuccessor(message.getIPAddress(), message.getPort());
        System.out.println("Successfully updated self successor with notification from true successor");

    }

    private void stabilize() {
        /*
         * this function is a routine loop
         * it will call its successor and get its predecessor -> GetPredecessorRequest
         * checks the result to see if it is still the successor ->
         * GetPredecessorResponse
         * 
         * if not,
         * update finger table and successor pointer
         * now send this new successor again a get pred request to see if it has
         * recorded me as its predecessor
         * 
         * start the notify process
         * send notifyyourpredecssor to new node
         * 
         * 
         * also perform task migration process when updating predecessor of this node
         */
    }

    public String getIPAddress() {
        return this.hostIP;
    }

    public int getPort() {
        return this.nodePort;
    }

    public int getPeerID() {
        return this.peerID;
    }

    private void printNeighbors() {
        /*
         * predecessor: <peerID> <ip-address>:<port>
         * successor: <peerID> <ip-address>:<port>
         */
        System.out.println("predecessor: " + fingerTable.getPredecessor().getHashCode() + " "
                + fingerTable.getPredecessor().getAddress() + " " + fingerTable.getPredecessor().getPort());

        System.out.println("successor: " + fingerTable.getSuccessor().getHashCode() + " "
                + fingerTable.getSuccessor().getAddress() + " " + fingerTable.getSuccessor().getPort());
    }

    private void printFiles() {
        /*
         * TODO: print the data structure containing file details
         * <file-name> <hash-code>
         */
        for (Map.Entry<String, Integer> entry : fingerTable.fileIndex.entrySet()) {
            String fileName = entry.getKey();
            Integer fileKey = entry.getValue();
            System.out.println(fileName + " " + fileKey);

        }
    }

}
