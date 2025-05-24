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
import java.util.Set;

public class EventLoopServer {
    private static final int BUFFER_SIZE = 1024;
    private static final int PORT = 6379;
    private static final boolean IS_MASTER = true;

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
            connectToMaster(commandParser.getMasterHost(), commandParser.getMasterPort());
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

    private static void connectToMaster(String host, int port) throws IOException {

        System.out.println("Sending ping to master");

        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.connect(new InetSocketAddress(host, port));


        String pingCommand = RedisSerializer.serialize(new Array(Arrays.asList(new BulkString("PING".getBytes()))));
        ByteBuffer byteBuffer = ByteBuffer.wrap(pingCommand.getBytes());
        socketChannel.write(byteBuffer);
        System.out.printf("Sent ping to master on host: %s, port: %d%n", host, port);
    }

    private static void handleAccept(Selector selector, SelectionKey key) throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
        SocketChannel channel = serverSocketChannel.accept();
        channel.configureBlocking(false);

        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        channel.register(selector, SelectionKey.OP_READ, buffer);
        System.out.println("Accepted connection from : "+channel.getRemoteAddress());
    }

    private static void handleRead(SelectionKey key) throws IOException{
        /**
         * Client channel retrieved from the key
         * get buffer from the key.attachment()
         * read from the client channel
         * buffer.flip()
         * read the buffer into array
         * convert array to string
         * Check input string
         * Write response to buffer
         */

        SocketChannel clientChannel = (SocketChannel) key.channel();
        ByteBuffer buffer = (ByteBuffer) key.attachment();

        buffer.clear();
        clientChannel.read(buffer);

        buffer.flip();

        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        String input = new String(data).trim();
        System.out.println("Input data received : "+ input);

        RedisObject parsedCommand = RedisParser.parse(input);
        RedisObject response = RedisCommandHandler.executeCommand(parsedCommand);
        // Write the redis parser here

        if(response != null) {
            String serverResponse = RedisSerializer.serialize(response);
            //serverResponse = serverResponse.replace("\r\n", "\\r\\n");
            System.out.println("Server response "+ serverResponse);
            buffer.clear();
            buffer.put(serverResponse.getBytes());
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
