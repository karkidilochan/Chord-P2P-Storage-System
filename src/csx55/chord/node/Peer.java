package csx55.chord.node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import csx55.chord.tcp.TCPConnection;
import csx55.chord.tcp.TCPServer;
import csx55.chord.wireformats.CheckStatus;
import csx55.chord.wireformats.ComputeNodesList;
import csx55.chord.wireformats.Event;
import csx55.chord.wireformats.MigrateResponse;
import csx55.chord.wireformats.Protocol;
import csx55.chord.wireformats.PushRequest;
import csx55.chord.wireformats.Register;
import csx55.chord.wireformats.RegisterResponse;
import csx55.chord.wireformats.SetupChord;
import csx55.chord.wireformats.TaskComplete;
import csx55.chord.wireformats.TaskInitiate;
import csx55.chord.wireformats.TasksCount;

import java.util.concurrent.TimeUnit;

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

    // Constants for command strings

    // create a TCP connection with the Registry
    private TCPConnection registryConnection;

    private TCPConnection predecessorConnection;
    private String predecessorHost;

    private TCPConnection successorConnection;
    private String successorHost;

    private Peer(String hostName, String hostIP, int nodePort, int peerID) {
        this.hostName = hostName;
        this.hostIP = hostIP;
        this.nodePort = nodePort;
        this.peerID = peerID;
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
            case Protocol.REGISTER_REQUEST:
                recordNewConnection((Register) event, connection);
                break;

            case Protocol.REGISTER_RESPONSE:
                handleRegisterResponse((RegisterResponse) event);
                break;

            case Protocol.SETUP_CHORD:
                createConnection((SetupChord) event);
                break;

            case Protocol.TASK_INITIATE:
                handleTaskInitiation((TaskInitiate) event);
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

    /**
     * Records a new connection established with another node.
     *
     * @param register   The Register message containing information about the new
     *                   connection.
     * @param connection The TCP connection established with the new node.
     */
    private void recordNewConnection(Register register, TCPConnection connection) {
        String nodeDetails = register.getConnectionReadable();

        // Store the connection in the connections map
        // this.incomingConnection = connection;
        // this.incomingConnectionHost = nodeDetails;
    }

    private void handleTaskInitiation(TaskInitiate taskInitiate) {

    }

    private void createConnection(SetupChord message) {

        try {
            Socket socketToPeer = new Socket(message.getIPAddress(), message.getPort());
            TCPConnection connection = new TCPConnection(this, socketToPeer);
            Register register = new Register(Protocol.REGISTER_REQUEST,
                    this.hostIP, this.nodePort, this.hostName, this.peerID);
            connection.getTCPSenderThread().sendData(register.getBytes());
            connection.start();
        } catch (IOException | InterruptedException e) {
            System.out.println(e.getMessage());
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
        }
    }

}
