package distributed.chord;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.Map;
import java.util.Scanner;

import distributed.tcp.TCPConnection;
import distributed.tcp.TCPServer;
import distributed.utils.Entry;
import distributed.utils.FixFingers;
import distributed.wireformats.DownloadRequest;
import distributed.wireformats.DownloadResponse;
import distributed.wireformats.Event;
import distributed.wireformats.FileNotFound;
import distributed.wireformats.FileTransfer;
import distributed.wireformats.FileTransferResponse;
import distributed.wireformats.FindSuccessorTypes;
import distributed.wireformats.GetPredecessor;
import distributed.wireformats.GetPredecessorResponse;
import distributed.wireformats.IdentifiedSuccessor;
import distributed.wireformats.NotifyYourPredecessor;
import distributed.wireformats.NotifyYourSuccessor;
import distributed.wireformats.Protocol;
import distributed.wireformats.Register;
import distributed.wireformats.RegisterResponse;
import distributed.wireformats.RequestSuccessor;
import distributed.wireformats.SetupChord;

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
    private Integer peerID;
    private final String hostIP;
    private final String fullAddress;

    private FingerTable fingerTable;
    private FixFingers fixFingers;

    private PeerUtilities utils;

    // Constants for command strings

    // create a TCP connection with the Registry
    private TCPConnection registryConnection;

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
        System.err.println("Usage: java distributed.chord.node.Peer discovery-ip discovery-port");
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

            this.utils = new PeerUtilities(this, fingerTable);

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
                        fingerTable.displayTable();
                        break;

                    case "fingertable":
                        fingerTable.print();
                        break;

                    case "upload":
                        utils.handleFileUpload(input[1]);
                        break;

                    case "download":
                        utils.handleFileDownload(input[1]);
                        break;

                    case "exit":
                        // TODO:
                        exitChord();
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
            System.out.println("Deregistering the node and terminating: " + hostName + ":" + nodePort);
            exitChord();
            System.exit(0);
        }
    }

    public void handleIncomingEvent(Event event, TCPConnection connection) {
        // System.out.println("Received event: " + event.toString());

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
                handleSuccessorResponse((IdentifiedSuccessor) event, connection);
                break;

            case Protocol.GET_PREDECESSOR:
                sendPredecessor((GetPredecessor) event, connection);
                break;

            case Protocol.GET_PREDECESSOR_RESPONSE:
                receivePredecessor((GetPredecessorResponse) event, connection);
                break;

            case Protocol.NOTIFY_SUCCESSOR:
                updatePredecessor((NotifyYourSuccessor) event, connection);
                break;

            case Protocol.NOTIFY_PREDECESSOR:
                updateSuccessor((NotifyYourPredecessor) event, connection);
                break;

            case Protocol.FILE_TRANSFER:
                utils.sendFileReceived((FileTransfer) event, connection);
                break;

            case Protocol.FILE_TRANSFER_RESPONSE:
                utils.handleFileTransferResponse((FileTransferResponse) event, connection);
                break;

            case Protocol.DOWNLOAD_REQUEST:
                utils.handleDownloadRequest((DownloadRequest) event, connection);
                break;

            case Protocol.DOWNLOAD_RESPONSE:
                utils.handleDownloadResponse((DownloadResponse) event, connection);
                break;

            case Protocol.FILE_NOT_FOUND:
                utils.handleFileNotFound((FileNotFound) event, connection);
                break;

            case Protocol.COLLISION:
                retryRegistration();
                break;

        }
    }

    private void retryRegistration() {
        try {
            // create a socket to the Registry server
            this.peerID = Math.abs(this.fullAddress.hashCode());

            Register register = new Register(Protocol.REGISTER_REQUEST,
                    this.hostIP, this.nodePort, this.hostName, this.peerID);

            // send "Register" message to the Registry
            this.registryConnection.getTCPSenderThread().sendData(register.getBytes());

            fingerTable.updatePeerId(this.peerID);
            /* initialize finger table */

        } catch (IOException | InterruptedException e) {
            System.out.println("Error registering node: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void handleRegisterResponse(RegisterResponse response) {
        this.fixFingers = new FixFingers(fingerTable, this);
        this.fixFingers.start();
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

    private void exitChord() {
        /*
         * while exiting, send all the files you were responsible for to your successor
         */
        utils.migrateFilesToSuccessor();
        this.fixFingers.stopRoutine();

        Register register = new Register(Protocol.DEREGISTER_REQUEST,
                this.hostIP, this.nodePort, this.hostName, this.peerID);

        Entry successor = fingerTable.getSuccessor();
        Entry predecessor = fingerTable.getPredecessor();

        /*
         * then notify your predecessor to update their successor with your successor
         * and notify your successor to update their predecessor with your predecessor
         */
        try {
            NotifyYourPredecessor notifyPred = new NotifyYourPredecessor(
                    successor.getAddress(), successor.getPort(), true);
            Socket socketToPred = new Socket(predecessor.getAddress(), predecessor.getPort());
            TCPConnection connection = new TCPConnection(this, socketToPred);
            connection.getTCPSenderThread().sendData(notifyPred.getBytes());
            connection.start();
        } catch (IOException | InterruptedException e) {
            System.out.println("Error occurred while notifying predecessor about their new successor"
                    + e.getMessage());
            e.printStackTrace();
        }

        try {
            NotifyYourSuccessor notifySucc = new NotifyYourSuccessor(predecessor.getAddress(),
                    predecessor.getPort(), true);
            Socket socketToSucc = new Socket(successor.getAddress(), successor.getPort());
            TCPConnection connection = new TCPConnection(this, socketToSucc);
            connection.getTCPSenderThread().sendData(notifySucc.getBytes());
            connection.start();
        } catch (IOException | InterruptedException e) {
            System.out.println("Error occurred while notifying predecessor about their new successor"
                    + e.getMessage());
            e.printStackTrace();
        }

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
        long lookupId = message.getLookupKey();
        message.addPeerToHops(this.peerID);
        message.incrementHops();

        /*
         * first look for the successor of lookup id in peer table
         * if found, send it back to the original peer
         * else
         * forward the successor request to node with closes succeeding id
         */
        try {
            peerConnection.close();

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

        } catch (IOException | InterruptedException e) {
            System.out.println(
                    "Error occured while forwarding request for successor." + "Forward count is " + e.getMessage());
            e.printStackTrace();

        }

    }

    private void handleSuccessorResponse(IdentifiedSuccessor message, TCPConnection connection) {
        int purpose = message.getPurpose();

        switch (purpose) {
            case FindSuccessorTypes.JOIN_REQUEST:
                utils.joinNetwork(message, connection);
                break;

            case FindSuccessorTypes.FILE_UPLOAD:
                utils.sendFileToPeer(message, connection);
                break;

            case FindSuccessorTypes.FILE_DOWNLOAD:
                utils.sendDownloadRequest(message, connection);
                break;

            case FindSuccessorTypes.FIX_FINGERS:
                utils.handleFixFingers(message, connection);

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

        /*
         * after getting predecessor, notify your successor that you are the new
         * predecessor
         */
        try {
            /* also notify this received predecessor that you are the new successor */
            NotifyYourPredecessor request = new NotifyYourPredecessor(
                    this.hostIP, this.nodePort, false);
            Socket socketToPredecessor = new Socket(message.getIPAddress(), message.getPort());
            TCPConnection predConnection = new TCPConnection(this, socketToPredecessor);
            predConnection.getTCPSenderThread().sendData(request.getBytes());
            predConnection.start();
        } catch (IOException | InterruptedException e) {
            System.out.println("Error occurred while notifying predecessor about this node." + e.getMessage());
            e.printStackTrace();
        }
        try {
            /* notify your successor that you are their new predecessor */
            NotifyYourSuccessor notify = new NotifyYourSuccessor(this.hostIP, this.nodePort, false);
            connection.getTCPSenderThread().sendData(notify.getBytes());

        } catch (IOException | InterruptedException e) {
            System.out.println("Error occurred while notifying successor about new predecessor." + e.getMessage());
            e.printStackTrace();
        }
    }

    private void updatePredecessor(NotifyYourSuccessor message, TCPConnection connection) {
        Entry oldPredecessor = fingerTable.getPredecessor();
        fingerTable.updatePredecessor(message.getIPAddress(), message.getPort());
        try {
            connection.close();

        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

        // TODO: now the successor will migrate files to its new predecessor
        // perform files migration process

        System.out.println("Successfully updated predecessor. Now migrating files to new predecessor.....");
        if (!message.checkIfExit()) {
            utils.migrateFilesToPredecessor(oldPredecessor);
        }
    }

    private void updateSuccessor(NotifyYourPredecessor message, TCPConnection connection) {
        fingerTable.updateSuccessor(message.getIPAddress(), message.getPort());
        try {
            connection.close();

        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
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

    public String getFullAddress() {
        return fullAddress;
    }

    private void printNeighbors() {
        /*
         * predecessor: <peerID> <ip-address>:<port>
         * successor: <peerID> <ip-address>:<port>
         */
        System.out.println("predecessor: " + fingerTable.getPredecessor().getHashCode() + " "
                + fingerTable.getPredecessor().getAddress() + ":" + fingerTable.getPredecessor().getPort());

        System.out.println("successor: " + fingerTable.getSuccessor().getHashCode() + " "
                + fingerTable.getSuccessor().getAddress() + ":" + fingerTable.getSuccessor().getPort());
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
