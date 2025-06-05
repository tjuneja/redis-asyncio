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
        if(args.length > 0) {

            if((args.length > 2) && args[2].contains("--replicaof")){
             port = Integer.parseInt(args[1]);
             RedisServerState.becomeFollower();

            }else{
                port = Integer.parseInt(args[1]);
                RedisServerState.becomeLeader();
            }
        }
        CommandParser commandParser = new CommandParser(args);
        System.out.println("Is Replica " + commandParser.isReplica());
        if(commandParser.isReplica()){
            connectToMaster(commandParser.getMasterHost(), commandParser.getMasterPort(), commandParser.getPort());
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

        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        channel.register(selector, SelectionKey.OP_READ, buffer);
        System.out.println("Accepted connection from : "+channel.getRemoteAddress());
    }

    private static void handleRead(SelectionKey key) throws IOException {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        ByteBuffer buffer = (ByteBuffer) key.attachment();

        buffer.clear();

        // This is the critical addition - check the return value!
        int bytesRead = clientChannel.read(buffer);

        // Handle different read scenarios
        if (bytesRead == -1) {
            // Client has closed the connection gracefully
            System.out.println("Client disconnected: " + clientChannel.getRemoteAddress());
            handleClientDisconnection(key);
            return;
        }

        if (bytesRead == 0) {
            // No data available right now in non-blocking mode
            // This shouldn't happen often since we're only called when data is ready
            System.out.println("No data available for reading");
            return;
        }

        // We have actual data to process
        buffer.flip();

        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        String input = new String(data).trim();
        System.out.println("Input data received : " + input);

        try {
            RedisObject parsedCommand = RedisParser.parse(input);
            CommandResponse response = RedisCommandHandler.executeCommand(parsedCommand);

            if(!response.isComplete() && response.isMultiPart()){
                writeMultiPartServerResponse(key, response, buffer);
            } else {
                writeServerResponse(key, response.getStringResponse(), buffer);
            }
        } catch (IOException e) {
            // Handle parsing errors gracefully
            System.err.println("Error parsing command: " + e.getMessage());
            String errorResponse = RedisSerializer.serialize(new objects.Error("ERR " + e.getMessage()));
            writeServerResponse(key, errorResponse, buffer);
        }
    }

    /**
     * Properly clean up when a client disconnects
     */
    private static void handleClientDisconnection(SelectionKey key) throws IOException {
        SocketChannel clientChannel = (SocketChannel) key.channel();

        // Log the disconnection for debugging
        System.out.println("Cleaning up disconnected client: " + clientChannel.getRemoteAddress());

        // Cancel the key to remove it from the selector
        key.cancel();

        // Close the channel
        clientChannel.close();

        // Note: The buffer attached to this key will be garbage collected automatically
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
