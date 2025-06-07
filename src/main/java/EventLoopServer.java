import connection.ConnectionManager;
import objects.Array;
import objects.BulkString;
import objects.RedisObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class EventLoopServer {
    private static final int BUFFER_SIZE = 1024;
    private static final int PORT = 6379;

    public static void main(String[] args) throws IOException {
        System.out.println("Event Loop started");
        int port = PORT;
        RedisServerState.becomeLeader();


        CommandParser commandParser = new CommandParser(args);
        System.out.println("Is Replica " + commandParser.isReplica());
        if(commandParser.isReplica()){
            port = commandParser.getPort();
            RedisServerState.becomeFollower();
            connectToMaster(commandParser.getMasterHost(), commandParser.getMasterPort(), commandParser.getPort());
        }else if(args.length > 0){
            port =Integer.parseInt(args[1]);
            RedisServerState.becomeLeader();
        }
        System.out.println("Starting a server at port : "+ port + " Role : "+RedisServerState.getStatus());

        //Create a server socket channel
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.socket().bind(new InetSocketAddress(port));
        serverChannel.socket().setReuseAddress(true);
        serverChannel.configureBlocking(false);

        // create a new selector
        Selector selector = Selector.open();
        // Register with the selector
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        //Event loop
        while(true){
            try{
                selector.select();
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> keyIterator = selectionKeys.iterator();

                while(keyIterator.hasNext()){
                    SelectionKey key = keyIterator.next();

                    if(key != null) keyIterator.remove();

                    if(key.isAcceptable()){
                        handleAccept(selector, key);
                    } else if (key.isReadable()) {
                        handleRead(key);
                    } else if (key.isWritable()) {
                        handleWrite(key);
                    }
                }
            }catch (IOException e) {
                System.err.println("Error in event loop: " + e.getMessage());
                e.printStackTrace();
            }


        }
    }

    private static void connectToMaster(String masterHost, int port, int currentServerPort) throws IOException {

        System.out.println("Sending ping to master");

        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.connect(new InetSocketAddress(masterHost, port));

        sendPingToMasterServer(socketChannel);
        sendReplConf(socketChannel, currentServerPort);
        System.out.printf("Sent ping to master on masterHost: %s, port: %d%n", masterHost, port);
    }

    private static void sendReplConf(SocketChannel socketChannel, int port) throws IOException {
        List<RedisObject> messageObjects = Arrays.asList(new BulkString("REPLCONF".getBytes())
                                            , new BulkString("listening-port".getBytes())
                                            , new BulkString(String.valueOf(port).getBytes()));
        Array messageArray = new Array(messageObjects);
        String replConfMessage = RedisSerializer.serialize(messageArray);
        ByteBuffer byteBuffer = ByteBuffer.wrap(replConfMessage.getBytes());
        socketChannel.write(byteBuffer);


        messageObjects = Arrays.asList(new BulkString("REPLCONF".getBytes())
                , new BulkString("capa".getBytes())
                , new BulkString("psync2".getBytes()));
        messageArray = new Array(messageObjects);
        replConfMessage = RedisSerializer.serialize(messageArray);
        byteBuffer = ByteBuffer.wrap(replConfMessage.getBytes());
        socketChannel.write(byteBuffer);


        receiveResponse(socketChannel);

        messageObjects = Arrays.asList(new BulkString("PSYNC".getBytes())
        , new BulkString("?".getBytes())
        , new BulkString("-1".getBytes()));
        messageArray = new Array(messageObjects);
        replConfMessage = RedisSerializer.serialize(messageArray);
        byteBuffer = ByteBuffer.wrap(replConfMessage.getBytes());
        socketChannel.write(byteBuffer);
    }

    private static void sendPingToMasterServer(SocketChannel socketChannel) throws IOException {
        String pingCommand = RedisSerializer.serialize(new Array(List.of(new BulkString("PING".getBytes()))));
        ByteBuffer byteBuffer = ByteBuffer.wrap(pingCommand.getBytes());
        socketChannel.write(byteBuffer);

        receiveResponse(socketChannel);
    }

    private static void receiveResponse(SocketChannel socketChannel) throws IOException {
        ByteBuffer responseBuffer = ByteBuffer.allocate(10240);
        int bytesRead = socketChannel.read(responseBuffer);
        String pingResponse = new String(responseBuffer.array(), 0, bytesRead);
        System.out.println("Received response from master: " + pingResponse);
        responseBuffer.clear();
    }

    private static void handleAccept(Selector selector, SelectionKey key) throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
        SocketChannel channel = serverSocketChannel.accept();
        channel.configureBlocking(false);

        ConnectionManager.registerClientConnection(channel);


        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        channel.register(selector, SelectionKey.OP_READ, buffer);
        System.out.println("Accepted connection from : "+channel.getRemoteAddress());
    }

    private static void handleRead(SelectionKey key) throws IOException {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        ByteBuffer buffer = (ByteBuffer) key.attachment();

        buffer.clear();
        int bytesRead = clientChannel.read(buffer);

        if (bytesRead == -1) {
            System.out.println("Client disconnected: " + clientChannel.getRemoteAddress());
            handleClientDisconnection(key);
            return;
        }

        if (bytesRead == 0) {
            System.out.println("No data available for reading");
            return;
        }

        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        String input = new String(data).trim();

        // Enhanced debugging for command processing
        System.out.println("=== COMMAND PROCESSING START ===");
        System.out.println("From: " + clientChannel.getRemoteAddress());
        System.out.println("Raw input: " + input);

        try {
            RedisObject parsedCommand = RedisParser.parse(input);

            // Debug: Show what command was parsed
            String commandName = extractCommandName(parsedCommand);
            System.out.println("Parsed command: " + commandName);

            // Debug: Show current connection type
            ConnectionManager.ConnectionType connectionType = ConnectionManager.getConnectionType(clientChannel);
            System.out.println("Connection type: " + connectionType);

            CommandResponse response;

            // Check if this is a replication handshake command
            if(isReplicationHandshake(parsedCommand, clientChannel)){
                System.out.println("Processing as REPLICATION HANDSHAKE command");
                response = RedisCommandHandler.executeCommand(parsedCommand);
                handleServerWrite(key, response, buffer);
                System.out.println("=== COMMAND PROCESSING END (handshake) ===");
                return;
            }

            // Process based on connection type
            if(connectionType == ConnectionManager.ConnectionType.MASTER){
                System.out.println("Processing command from MASTER (silent mode)");
                response = RedisCommandHandler.executeCommand(parsedCommand);
                System.out.println("Command executed silently, no response sent to master");
                System.out.println("=== COMMAND PROCESSING END (from master) ===");
                return; // Don't send response to master
            } else {
                System.out.println("Processing command from CLIENT");
                response = RedisCommandHandler.executeCommand(parsedCommand);

                // Check if we should propagate this command
                if(RedisServerState.isLeader()){
                    System.out.println("Server is leader - checking if command should be propagated");
                    CommandPropagator.propagateCommand(parsedCommand);
                } else {
                    System.out.println("Server is not leader - no propagation");
                }
            }

            handleServerWrite(key, response, buffer);
            System.out.println("=== COMMAND PROCESSING END (client response sent) ===");

        } catch (IOException e) {
            System.err.println("Error parsing command: " + e.getMessage());
            String errorResponse = RedisSerializer.serialize(new objects.Error("ERR " + e.getMessage()));
            writeServerResponse(key, errorResponse, buffer);
            System.out.println("=== COMMAND PROCESSING END (error) ===");
        }
    }

    /**
     * Helper method to extract command name for debugging
     */
    private static String extractCommandName(RedisObject parsedCommand) {
        if(!(parsedCommand instanceof Array commands)) {
            return "NOT_ARRAY";
        }

        List<RedisObject> elements = commands.getElements();
        if(elements == null || elements.isEmpty()) {
            return "EMPTY_ARRAY";
        }

        if(!(elements.get(0) instanceof BulkString firstCommand)) {
            return "NOT_BULK_STRING";
        }

        return firstCommand.getValueAsString().toUpperCase();
    }

    private static void handleServerWrite(SelectionKey key, CommandResponse response, ByteBuffer buffer) throws IOException {
        if(!response.isComplete() && response.isMultiPart()){
            writeMultiPartServerResponse(key, response, buffer);
        } else {
            writeServerResponse(key, response.getStringResponse(), buffer);
        }
    }

    private static boolean isReplicationHandshake(RedisObject parsedCommand, SocketChannel clientChannel){
        if (!(parsedCommand instanceof Array commands)) {
            return false;
        }

        List<RedisObject> elements = commands.getElements();
        if (elements == null || elements.isEmpty()) {
            return false;
        }

        if (!(elements.get(0) instanceof BulkString commandName)) {
            return false;
        }

        String command = commandName.getValueAsString().toUpperCase();

        if("REPLCONF".equals(command) || "PSYNC".equals(command)){
            System.out.println("Handling replication command: " + command);

            // Upgrade this connection to a replica connection
            if (ConnectionManager.getConnectionType(clientChannel) == ConnectionManager.ConnectionType.CLIENT) {
                ConnectionManager.removeConnection(clientChannel);
                ConnectionManager.registerReplicaConnection(clientChannel);
                System.out.println("Upgraded connection to replica type");
            }

            return true;
        }

        return false;
    }

    /**
     * Properly clean up when a client disconnects
     */
    private static void handleClientDisconnection(SelectionKey key) throws IOException {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        ConnectionManager.removeConnection(clientChannel);
        // Log the disconnection for debugging
        System.out.println("Cleaning up disconnected client: " + clientChannel.getRemoteAddress());

        // Cancel the key to remove it from the selector
        key.cancel();

        // Close the channel
        clientChannel.close();

        // The buffer attached to this key will be garbage collected automatically
    }

    private static void writeMultiPartServerResponse(SelectionKey key, CommandResponse response, ByteBuffer buffer) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();

        for (CommandResponse.ResponsePart responsePart : response.getParts()) {
            buffer.clear();
            buffer.put(responsePart.getData());
            buffer.flip();

            while (buffer.hasRemaining()){
                channel.write(buffer);
            }

            System.out.printf("Sent %s part: %d bytes%n",
                    responsePart.getType(), responsePart.getData().length);
        }

        buffer.clear();
        key.interestOps(SelectionKey.OP_READ);
    }

    private static void writeServerResponse(SelectionKey key, String response, ByteBuffer buffer) {
        if(response != null) {
            //serverResponse = serverResponse.replace("\r\n", "\\r\\n");
            System.out.println("Server response "+ response);
            buffer.clear();
            buffer.put(response.getBytes());
            buffer.flip();

            key.interestOps(SelectionKey.OP_WRITE);
        }
    }


    private static void handleWrite( SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ByteBuffer byteBuffer = (ByteBuffer) key.attachment();

        channel.write(byteBuffer);

        if(!byteBuffer.hasRemaining()){
            byteBuffer.clear();
            key.interestOps(SelectionKey.OP_READ);
        }
    }


}
