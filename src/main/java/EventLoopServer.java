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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class EventLoopServer {
    private static final int BUFFER_SIZE = 1024;
    private static final int PORT = 6379;
    private static final Map<SocketChannel, ConnectionBuffer> streamingBuffers =
            new ConcurrentHashMap<>();

    public static class ConnectionBuffer{
        StringBuilder accumulator = new StringBuilder();
        SocketChannel channel;

        public ConnectionBuffer(SocketChannel channel){
            this.channel = channel;
        }

        public void appendData(String data){
            this.accumulator.append(data);
        }

        public SocketChannel getChannel(){
            return this.channel;
        }

        public String getAccumulatedData(){
            return this.accumulator.toString();
        }

        public void consumeData(int charactersConsumed){
            if(charactersConsumed >= accumulator.length()){
                this.accumulator.setLength(0); // clear all
            }else{
                String remaining = this.accumulator.substring(charactersConsumed);
                accumulator.setLength(0);
                accumulator.append(remaining);
            }
        }

        public boolean hasData() {
            return !accumulator.isEmpty();
        }
    }

    public static class RespParseResult {
        private final String consumedData;
        private final boolean complete;
        private final int consumedLength;

        public RespParseResult(boolean complete, String consumedData){
            this.consumedData = consumedData;
            this.complete = complete;
            this.consumedLength = consumedData.length();
        }


        public String getConsumedData() {
            return consumedData;
        }

        public int getConsumedLength() {
            return consumedLength;
        }

        public boolean isComplete() {
            return complete;
        }
    }




    public static void main(String[] args) throws IOException {
        System.out.println("Event Loop started");
        int port = PORT;
        RedisServerState.becomeLeader();
        SocketChannel masterConnection = null;

        CommandParser commandParser = new CommandParser(args);
        System.out.println("Is Replica " + commandParser.isReplica());
        if(commandParser.isReplica()){
            port = commandParser.getPort();
            RedisServerState.becomeFollower();
            masterConnection = connectToMaster(commandParser.getMasterHost(), commandParser.getMasterPort(), commandParser.getPort());
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

        if(masterConnection != null){
            ByteBuffer masterBuffer = ByteBuffer.allocate(BUFFER_SIZE);
            masterConnection.register(selector, SelectionKey.OP_READ, masterBuffer);
            System.out.println("Master connection registered with selector");
        }

        //Event loop
        while(true){
            try{
                selector.select();
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> keyIterator = selectionKeys.iterator();

                // In your main event loop, enhance the key processing
                while(keyIterator.hasNext()){
                    SelectionKey key = keyIterator.next();
                    if(key != null) keyIterator.remove();

                    SocketChannel channel = null;
                    if (key.channel() instanceof SocketChannel) {
                        channel = (SocketChannel) key.channel();
                    }

                    System.out.println("=== EVENT LOOP ITERATION ===");
                    if (channel != null) {
                        System.out.println("Channel: " + channel.getRemoteAddress());
                        System.out.println("Channel open: " + channel.isOpen());
                        System.out.println("Channel connected: " + channel.isConnected());
                    }
                    System.out.println("Key valid: " + key.isValid());
                    System.out.println("Operations - Accept: " + key.isAcceptable() +
                            ", Read: " + key.isReadable() +
                            ", Write: " + key.isWritable());

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

    private static SocketChannel connectToMaster(String masterHost, int port, int currentServerPort) throws IOException {

        System.out.println("Sending ping to master");

        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.connect(new InetSocketAddress(masterHost, port));

        sendPingToMasterServer(socketChannel);
        sendReplConf(socketChannel, currentServerPort);
        ConnectionManager.setMasterConnection(socketChannel);
        socketChannel.configureBlocking(false);
        System.out.printf("Sent ping to master on masterHost: %s, port: %d%n", masterHost, port);
        return socketChannel;
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
        ByteBuffer responseBuffer = (ByteBuffer)key.attachment();

        if(responseBuffer == null){
            responseBuffer = ByteBuffer.allocate(1024);
            key.attach(responseBuffer);
        }
        responseBuffer.clear();

        int bytesRead = clientChannel.read(responseBuffer);

        ConnectionBuffer streamBuffer = streamingBuffers.computeIfAbsent(
                clientChannel, ConnectionBuffer::new);

        if (bytesRead == -1) {
            System.out.println("Client disconnected: " + clientChannel.getRemoteAddress());
            handleClientDisconnection(key);
            return;
        }

        if (bytesRead == 0) {
            System.out.println("No data available for reading");
            return;
        }

        responseBuffer.flip();
        byte[] data = new byte[responseBuffer.remaining()];
        responseBuffer.get(data);
        String input = new String(data);

        streamBuffer.appendData(input);
        // Enhanced debugging for command processing
        System.out.println("=== COMMAND PROCESSING START ===");
        System.out.println("From: " + clientChannel.getRemoteAddress());
        System.out.println("Raw input: " + input);

        parseAccumulatedCommand(key, streamBuffer, responseBuffer);
    }


    private static void parseAccumulatedCommand(SelectionKey key, ConnectionBuffer connectionBuffer, ByteBuffer responseBuffer) {
        String accumulatedData = connectionBuffer.getAccumulatedData();
        int totalConsumedData = 0;
        int commandCount = 0;

        System.out.println("=== PARSE ACCUMULATED COMMANDS START ===");
        System.out.println("Total accumulated data: " + accumulatedData.length() + " characters");

        // First, skip any non-array messages (handshake responses)
        int arrayStart = findNextArrayStart(accumulatedData);
        if (arrayStart > 0) {
            String skippedData = accumulatedData.substring(0, Math.min(arrayStart, 100));
            System.out.println("Skipping " + arrayStart + " characters of handshake data");
            System.out.println("Handshake data preview: " + skippedData.replace("\r", "\\r").replace("\n", "\\n") + "...");

            // Remove the handshake data from our buffer
            connectionBuffer.consumeData(arrayStart);
            accumulatedData = connectionBuffer.getAccumulatedData();

            System.out.println("After skipping handshake: " + accumulatedData.length() + " characters remaining");
        }

        // Now process only array commands (commands that start with '*')
        while (true) {
            String remainingData = accumulatedData.substring(totalConsumedData);

            if (remainingData.isEmpty()) {
                System.out.println("No more data to process");
                break;
            }

            // Additional safety check: ensure we're looking at an array command
            if (!remainingData.startsWith("*")) {
                System.out.println("Found non-array data after handshake skip - this shouldn't happen");
                System.out.println("Problematic data: " + remainingData.substring(0, Math.min(50, remainingData.length())));

                // Try to find the next array start and skip to it
                int nextArrayStart = findNextArrayStart(remainingData);
                if (nextArrayStart == remainingData.length()) {
                    // No more arrays found, consume all remaining data
                    totalConsumedData = accumulatedData.length();
                    break;
                } else {
                    // Skip to the next array
                    totalConsumedData += nextArrayStart;
                    continue;
                }
            }

            try {
                // Parse one complete array command
                RespParseResult parsedResult = parseOneRespCommand(remainingData);

                if (!parsedResult.isComplete()) {
                    System.out.println("Incomplete array command found - waiting for more data");
                    System.out.println("Partial command: " + remainingData.substring(0, Math.min(50, remainingData.length())).replace("\r", "\\r").replace("\n", "\\n") + "...");
                    break; // Wait for more data
                }

                commandCount++;
                System.out.println("=== PROCESSING ARRAY COMMAND #" + commandCount + " ===");
                System.out.println("Command data: " + parsedResult.getConsumedData().replace("\r", "\\r").replace("\n", "\\n"));

                // Parse and execute this Redis command
                RedisObject parsedCommand = RedisParser.parse(parsedResult.getConsumedData());
                parseIndividualCommand(key, parsedCommand, connectionBuffer.getChannel(), responseBuffer);

                totalConsumedData += parsedResult.getConsumedLength();

            } catch (Exception e) {
                System.err.println("Error parsing array command: " + e.getMessage());
                System.err.println("Problematic data: " + remainingData.substring(0, Math.min(100, remainingData.length())));

                // Skip this malformed data to prevent infinite loops
                totalConsumedData = accumulatedData.length();
                break;
            }
        }

        // Remove all processed data from the buffer
        connectionBuffer.consumeData(totalConsumedData);

        System.out.println("=== BATCH PROCESSING COMPLETE ===");
        System.out.println("Array commands processed: " + commandCount);
        System.out.println("Data consumed: " + totalConsumedData + " characters");
        System.out.println("Remaining in buffer: " + connectionBuffer.getAccumulatedData().length() + " characters");

        if (connectionBuffer.hasData()) {
            String remaining = connectionBuffer.getAccumulatedData();
            System.out.println("Remaining data preview: " +
                    remaining.substring(0, Math.min(50, remaining.length())).replace("\r", "\\r").replace("\n", "\\n") + "...");
        }
    }

    /**
     * Finds the position of the next array command (starting with '*') in the data.
     * This helps us skip over handshake responses and focus only on propagated commands.
     *
     * @param data The accumulated data to search through
     * @return The index of the first '*' character, or data.length() if no array found
     */
    private static int findNextArrayStart(String data) {
        for (int i = 0; i < data.length(); i++) {
            if (data.charAt(i) == '*') {
                // Found the start of an array command
                System.out.println("Found array start at position " + i);
                return i;
            }
        }

        // No array command found in the current data
        System.out.println("No array commands found in current data");
        return data.length(); // Return length to indicate "skip all current data"
    }

    private static void parseIndividualCommand(SelectionKey key, RedisObject parsedCommand, SocketChannel clientChannel, ByteBuffer buffer) {
        try {

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
                return;
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
        System.out.println("=== HANDLE SERVER WRITE ===");
        System.out.println("Response complete: " + response.isComplete());
        System.out.println("Response multipart: " + response.isMultiPart());

        if(!response.isComplete() && response.isMultiPart()){
            System.out.println("Taking MULTI-PART path");
            writeMultiPartServerResponse(key, response, buffer);
        } else {
            System.out.println("Taking SINGLE RESPONSE path");
            writeServerResponse(key, response.getStringResponse(), buffer);
        }
        System.out.println("=== HANDLE SERVER WRITE COMPLETE ===");
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
        streamingBuffers.remove(clientChannel);
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
            System.out.println("=== DETAILED RESPONSE ANALYSIS ===");
            System.out.println("Response string length: " + response.length());
            System.out.println("Response as chars: " + response.replace("\r", "\\r").replace("\n", "\\n"));

            // Show each byte value
            byte[] responseBytes = response.getBytes();
            System.out.print("Response bytes: [");
            for (int i = 0; i < responseBytes.length; i++) {
                System.out.print(responseBytes[i]);
                if (i < responseBytes.length - 1) System.out.print(", ");
            }
            System.out.println("]");

            buffer.clear();
            buffer.put(response.getBytes());
            buffer.flip();

            key.interestOps(SelectionKey.OP_WRITE);
        }
    }


    private static void handleWrite(SelectionKey key) throws IOException {
        System.out.println("=== HANDLE WRITE WITH CONNECTION VALIDATION ===");

        SocketChannel channel = (SocketChannel) key.channel();
        ByteBuffer buffer = (ByteBuffer) key.attachment();

        // Validate connection state before attempting write
        System.out.println("Pre-write validation:");
        System.out.println("  Channel open: " + channel.isOpen());
        System.out.println("  Channel connected: " + channel.isConnected());
        System.out.println("  Key valid: " + key.isValid());
        System.out.println("  Remote address: " + channel.getRemoteAddress());

        if (!channel.isOpen() || !channel.isConnected()) {
            System.err.println("ERROR: Attempting to write to closed/disconnected channel!");
            key.cancel();
            return;
        }

        if (buffer == null || !buffer.hasRemaining()) {
            System.err.println("ERROR: No data to write or buffer is null!");
            key.interestOps(SelectionKey.OP_READ);
            return;
        }

        try {
            int bytesWritten = channel.write(buffer);
            System.out.println("Successfully wrote " + bytesWritten + " bytes");

            // Validate connection state after write
            System.out.println("Post-write validation:");
            System.out.println("  Channel still open: " + channel.isOpen());
            System.out.println("  Channel still connected: " + channel.isConnected());

            if (!buffer.hasRemaining()) {
                System.out.println("All data written - returning to read mode");
                buffer.clear();
                key.interestOps(SelectionKey.OP_READ);
            }

        } catch (IOException e) {
            System.err.println("IOException during write - connection likely closed by client");
            System.err.println("Error: " + e.getMessage());

            // Clean up the failed connection
            handleClientDisconnection(key);
            throw e;
        }

        System.out.println("=== HANDLE WRITE VALIDATION COMPLETE ===");
    }

    private static RespParseResult parseOneRespCommand(String data){
        if(data.isEmpty() || !data.startsWith("*")) return new RespParseResult(false, "");

        try{
            int firstCRLF = data.indexOf("\r\n");
            if(firstCRLF == -1) return new RespParseResult(false, "");

            int arrLen = Integer.parseInt(data.substring(1,firstCRLF));
            int pos = firstCRLF +2;

            for(int i = 0 ; i< arrLen; i++){
                if(pos >= data.length() || data.charAt(pos) != '$'){
                    return new RespParseResult(false, "");
                }

                int nextCRLF = data.indexOf("\r\n", pos);
                if (nextCRLF == -1) return new RespParseResult(false, "");
                int bulkLength = Integer.parseInt(data.substring(pos+1,nextCRLF));
                pos = nextCRLF+2;

                if(pos+bulkLength+2 > data.length()) return new RespParseResult(false, "");

                pos += bulkLength+2;
            }
            return new RespParseResult(true, data.substring(0, pos));

        } catch (Exception e) {
            return new RespParseResult(false, "");
        }

    }

}
