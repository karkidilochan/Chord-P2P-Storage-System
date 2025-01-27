package distributed.chord;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Iterator;

import distributed.tcp.TCPConnection;
import distributed.utils.Entry;
import distributed.wireformats.DownloadRequest;
import distributed.wireformats.DownloadResponse;
import distributed.wireformats.FileNotFound;
import distributed.wireformats.FileTransfer;
import distributed.wireformats.FileTransferResponse;
import distributed.wireformats.FindSuccessorTypes;
import distributed.wireformats.GetPredecessor;
import distributed.wireformats.IdentifiedSuccessor;
import distributed.wireformats.Protocol;
import distributed.wireformats.RequestSuccessor;

public class PeerUtilities {

    private Peer peer;
    private FingerTable fingerTable;

    public PeerUtilities(Peer peer, FingerTable fingerTable) {
        this.peer = peer;
        this.fingerTable = fingerTable;
    }

    public void handleDownloadRequest(DownloadRequest message, TCPConnection connection) {
        try {

            /* payload of message contains file name */
            String fileName = message.getFileName();

            /*
             * first check if the file exists
             * if it doesn't send a file not found event
             * else send a download response with file as payload
             */
            String filePath = "/tmp/" + peer.getPeerID() + "/" + fileName;
            File file = new File(filePath);

            if (file.exists()) {
                DownloadResponse request = new DownloadResponse(fileName, message.getHopsCount(), message.getHopList(),
                        Files.readAllBytes(file.toPath()));
                System.out.println(request.getPayload());
                connection.getTCPSenderThread().sendData(request.getBytes());
            } else {
                FileNotFound request = new FileNotFound("Response from" + peer.getFullAddress() + " " + peer.getPeerID()
                        + ".The file " + fileName + " " + fileName.hashCode() + " cannot be found.");
                connection.getTCPSenderThread().sendData(request.getBytes());

            }

        } catch (IOException | InterruptedException e) {
            System.out.println(
                    "Error occurred handling download request and sending response to peer: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void handleDownloadResponse(DownloadResponse message, TCPConnection connection) {
        try {

            byte status;
            String response;
            System.out.println(
                    "File received successfully in this consecutive order in hops count: " + message.getHopsCount());

            boolean isSuccessful = writeFileCurrentDirectory(message);
            if (isSuccessful) {
                status = Protocol.SUCCESS;
                response = "File download was successful.";
            } else {
                status = Protocol.FAILURE;
                response = "File download failed.";
            }

            FileTransferResponse request = new FileTransferResponse(status, response);
            connection.getTCPSenderThread().sendData(request.getBytes());

        } catch (IOException | InterruptedException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    public boolean writeFileCurrentDirectory(DownloadResponse message) {
        boolean isSuccessful;
        try {
            // File currentDirectory = new File(".");
            File currentDirectory = new File(".");
            byte[] filePayload = message.getPayload();
            Files.write(Paths.get(currentDirectory.getAbsolutePath(), message.getFileName()), filePayload);
            System.out.println("Successfully downloaded requested file to current working directory.");
            System.out.println(message.getHopList());
            isSuccessful = true;
            return isSuccessful;
        } catch (Exception e) {
            System.out.println("Error occurred while writing file to current working directory: " + e.getMessage());
            e.printStackTrace();
            isSuccessful = false;
            return isSuccessful;
        }
    }

    public void handleFileNotFound(FileNotFound message, TCPConnection connection) {
        System.out.println(message.getMessage());
        try {
            connection.close();

        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    public void handleFileDownload(String fileName) {
        /*
         * check if you are responsible for the file key
         * if not send find successor request to appropriate predecessor
         */
        int fileKey = Math.abs(fileName.hashCode());

        File uploadDirectory = new File("/tmp/" + peer.getPeerID());

        try {
            if (fingerTable.isWithinRing(fileKey, fingerTable.getPredecessor().getHashCode(), peer.getPeerID())) {
                if (!uploadDirectory.exists()) {
                    System.out.println("Error occurred while trying to read file, file doesn't exist.");
                    return;
                }
                File currentDirectory = new File(".");
                byte[] filePayload = Files
                        .readAllBytes(Paths.get("/tmp", String.valueOf(peer.getPeerID()), fileName));
                Files.write(Paths.get(currentDirectory.getAbsolutePath(), fileName), filePayload);
                System.out.println("Successfully downloaded requested file to current working directory.");
            } else {
                Entry lookupResult = fingerTable.lookup(fileKey);

                /* get its successor */
                Socket socketToPred = new Socket(lookupResult.getAddress(),
                        lookupResult.getPort());
                TCPConnection connectionToPred = new TCPConnection(peer, socketToPred);

                RequestSuccessor request = new RequestSuccessor(
                        FindSuccessorTypes.FILE_DOWNLOAD, fileName, fileKey, peer.getIPAddress(), peer.getPort());
                request.addPeerToHops(peer.getPeerID());
                connectionToPred.getTCPSenderThread().sendData(request.getBytes());
                connectionToPred.start();
            }
        } catch (IOException | InterruptedException e) {
            System.out.println("Error occurred while handling file download: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void handleFileUpload(String filePath) {
        File uploadFile = new File(filePath);

        String filename = uploadFile.getName();
        int fileKey = Math.abs(filename.hashCode());

        /*
         * Workflow:
         * get 32-bit key for filename
         * perform lookup to find the successor(k)
         * forward a file upload request until the correct successor is found (same as
         * the node joining process)
         * the correct peer handles the upload request and stores it in /tmp/<peerID>/.
         * 
         */

        try {
            /*
             * first look for the successor of lookup id in peer table
             * if found, send it back to the original peer
             * else
             * forward the successor request to node with closes succeeding id
             */
            if (fingerTable.isWithinRing(fileKey, fingerTable.getPredecessor().getHashCode(), peer.getPeerID())) {
                /* this node is the successor of k, so it handles the upload */
                File fileToUpload = new File(filePath);
                writeFile(peer.getPeerID(), fileToUpload.getName(), Files.readAllBytes(fileToUpload.toPath()));
            } else {
                /*
                 * now forward the find successor request to closest succeeding id
                 * for this find the closest predecessor and ping to get its successor
                 */

                // call fingertable lookup function to find the predecessor

                Entry lookupResult = fingerTable.lookup(fileKey);

                /* get its successor */
                Socket socketToPred = new Socket(lookupResult.getAddress(),
                        lookupResult.getPort());
                TCPConnection connectionToPred = new TCPConnection(peer, socketToPred);

                RequestSuccessor message = new RequestSuccessor(
                        FindSuccessorTypes.FILE_UPLOAD, filePath, fileKey, peer.getIPAddress(), peer.getPort());
                message.addPeerToHops(peer.getPeerID());
                connectionToPred.getTCPSenderThread().sendData(message.getBytes());
                connectionToPred.start();

            }

        } catch (IOException | InterruptedException e) {
            System.out.println(
                    "Error occurred while handling file upload" + e.getMessage());
            e.printStackTrace();

        }
    }

    public void sendFileToPeer(IdentifiedSuccessor message, TCPConnection connection) {

        try {
            /*
             * read the file from the file path in the payload
             * then send transfer it to the identified peer
             */
            // Socket socket = new Socket(message.getIPAddress(),
            // message.getPort());
            // TCPConnection connection = new TCPConnection(peer, socket);

            /* payload of message contains file path */
            File fileToUpload = new File(message.getPayload());
            FileTransfer request = new FileTransfer(fileToUpload.getName(),
                    Files.readAllBytes(fileToUpload.toPath()));
            connection.getTCPSenderThread().sendData(request.getBytes());
            // connection.start();
        } catch (IOException | InterruptedException e) {
            System.out.println("Error occurred while sending file to peer: " + e.getMessage());
            e.printStackTrace();
        }

    }

    public void sendFileReceived(FileTransfer message, TCPConnection connection) {
        /* deserialize the file and put it in the //tmp/peerid directory */
        byte status;
        String response;
        boolean isSuccessful;
        try {

            isSuccessful = writeFile(peer.getPeerID(), message.getFileName(), message.getFilePayload());

            if (isSuccessful) {
                status = Protocol.SUCCESS;
                response = "File upload was successful.";
            } else {
                status = Protocol.FAILURE;
                response = "File upload failed.";
            }

            FileTransferResponse request = new FileTransferResponse(status, response);
            connection.getTCPSenderThread().sendData(request.getBytes());
        } catch (IOException | InterruptedException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

    }

    public void sendDownloadRequest(IdentifiedSuccessor message, TCPConnection connectionToPeer) {

        try {
            // Socket socket = new Socket(message.getIPAddress(),
            // message.getPort());
            // TCPConnection connectionToPeer = new TCPConnection(peer, socket);

            DownloadRequest request = new DownloadRequest(message.getPayload(), message.getHopsCount(),
                    message.getHopList());
            connectionToPeer.getTCPSenderThread().sendData(request.getBytes());
        } catch (IOException | InterruptedException e) {
            System.out.println("Error sending download request: " + e.getMessage());
            e.printStackTrace();
        }

    }

    /* call this function when current peer is the successor of the filekey */
    public boolean writeFile(int nodeID, String fileName, byte[] filePayload) {
        /* TODO: you are sending files through sockets */
        /* create /tmp/<peerID> if it doesn't exist */

        String uploadPath = "/tmp/" + nodeID;
        File uploadDirectory = new File(uploadPath);

        if (!uploadDirectory.exists()) {
            System.out.println("Upload directory doesn't exist. Creating...");
            uploadDirectory.mkdirs();
        }

        try {
            Path filePath = Paths.get("/tmp", String.valueOf(nodeID), fileName);
            if (!Files.exists(filePath)) {
                // Create a new file
                Files.createFile(filePath);
            }
            Files.write(filePath, filePayload);
            System.out.println("Successfully uploaded file " + fileName + " at: " + filePath.toAbsolutePath());
        } catch (IOException e) {
            System.out.println("Error occurred while trying to upload file: " +
                    e.getMessage());
            e.printStackTrace();
            return false;
        }

        fingerTable.fileIndex.put(fileName, Math.abs(fileName.hashCode()));

        return true;

    }

    public void handleFileTransferResponse(FileTransferResponse message, TCPConnection connection) {
        System.out.println("Received file transfer response from the peer: " + message.toString());
        try {
            connection.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    public void joinNetwork(IdentifiedSuccessor message,
            TCPConnection connection) {
        /*
         * after getting your successor
         * contact successor
         * and contact your successor's predecessor
         */
        // update finger table and successor
        fingerTable.updateSuccessor(message.getIPAddress(), message.getPort());

        // query pred of this successor and make it your predecessor
        // also notify your successor you are its pred

        try {

            // Socket socketToSuccessor = new Socket(message.getIPAddress(),
            // message.getPort());
            // TCPConnection successorConnection = new TCPConnection(peer,
            // socketToSuccessor);

            GetPredecessor request = new GetPredecessor(peer.getIPAddress(), peer.getPort());

            connection.getTCPSenderThread().sendData(request.getBytes());
            // successorConnection.start();
        } catch (IOException | InterruptedException e) {
            System.out.println("Error occurred while requesting predecessor info." + e.getMessage());
            e.printStackTrace();
        }
    }

    public void migrateFilesToPredecessor(Entry oldPredecessor) {
        /*
         * send those files whose file key is within ring of old predecessor and current
         * predecessor key values
         */
        Entry predecessor = fingerTable.getPredecessor();
        Iterator<Map.Entry<String, Integer>> iterator = fingerTable.getFileIndex().entrySet().iterator();

        while (iterator.hasNext()) {
            // for (Map.Entry<String, Integer> index :
            // fingerTable.getFileIndex().entrySet()) {
            Map.Entry<String, Integer> index = iterator.next();
            String fileName = index.getKey();
            Integer fileKey = index.getValue();
            String filePath = "/tmp/" + peer.getPeerID() + "/" + fileName;

            File fileToUpload = new File(filePath);
            try {

                if (fingerTable.isWithinRing(fileKey, oldPredecessor.getHashCode(),
                        predecessor.getHashCode())) {
                    Socket socket = new Socket(predecessor.getAddress(),
                            predecessor.getPort());
                    TCPConnection connection = new TCPConnection(peer, socket);
                    connection.start();
                    boolean isSuccessful = migrateFile(connection, fileToUpload);
                    if (isSuccessful) {
                        iterator.remove();
                        // fingerTable.fileIndex.remove(index.getKey());
                        fileToUpload.delete();

                        System.out.println("Removed file " + fileName + " after migration to "
                                + fingerTable.getPredecessor().getEntryString());
                    }

                }

            } catch (Exception e) {
                System.out.println("Error occured while sending files to predecessor: " + e.getMessage());
                e.printStackTrace();
            }

        }

    }

    public void migrateFilesToSuccessor() {
        /*
         * send those files whose file key is within ring of old predecessor and current
         * predecessor key values
         */
        Iterator<Map.Entry<String, Integer>> iterator = fingerTable.getFileIndex().entrySet().iterator();

        Entry successor = fingerTable.getSuccessor();
        while (iterator.hasNext()) {

            // for (Map.Entry<String, Integer> index :
            // fingerTable.getFileIndex().entrySet()) {
            Map.Entry<String, Integer> index = iterator.next();

            String fileName = index.getKey();
            String filePath = "/tmp/" + peer.getPeerID() + "/" + fileName;

            File fileToUpload = new File(filePath);
            try {
                Socket socket = new Socket(successor.getAddress(),
                        successor.getPort());
                TCPConnection connection = new TCPConnection(peer, socket);
                connection.start();
                boolean isSuccessful = migrateFile(connection, fileToUpload);
                if (isSuccessful) {
                    iterator.remove();

                    // fingerTable.fileIndex.remove(index.getKey());
                    fileToUpload.delete();
                    System.out.println("Removed file " + fileName + " after migration to "
                            + fingerTable.getPredecessor().getEntryString());
                }

            } catch (Exception e) {
                System.out.println("Error while migrating files to successor." + e.getMessage());
                e.printStackTrace();
            }

        }
    }

    public boolean migrateFile(TCPConnection connection, File fileToUpload) {
        boolean isSuccessful;
        try {
            /*
             * read the file from the file path in the payload
             * then send transfer it to the identified peer
             */

            /* payload of message contains file path */

            FileTransfer request = new FileTransfer(fileToUpload.getName(),
                    Files.readAllBytes(fileToUpload.toPath()));
            connection.getTCPSenderThread().sendData(request.getBytes());
            isSuccessful = true;
            return isSuccessful;
        } catch (IOException | InterruptedException e) {
            System.out.println("Error occurred while sending file to peer: " + e.getMessage());
            e.printStackTrace();
            isSuccessful = false;
            return isSuccessful;
        }

    }

    public synchronized void handleFixFingers(IdentifiedSuccessor message,
            TCPConnection connection) {
        /*
         * payload contains the index of fingertable
         * check the node at that index and see if its same with the identified
         * successor
         * if its same no need to update
         * else update with new successor
         */
        int index = Integer.valueOf(message.getPayload());
        Entry existingEntry = fingerTable.getTable().get(index);

        if (!existingEntry.getEntryString().equals(message.getConnectionReadable())) {
            Entry newEntry = new Entry(existingEntry.getRingPosition(), message.getIPAddress(), message.getPort());
            fingerTable.getTable().add(index, newEntry);
        }
        try {
            connection.close();

        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

    }

}
