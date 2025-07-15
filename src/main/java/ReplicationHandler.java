import connection.ConnectionManager;
import objects.Array;
import objects.BulkString;
import objects.RedisObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ReplicationHandler {
    private static final int HANDSHAKE_TIMEOUT_MS = 5000;

    public boolean isReplicaHandshake(RedisObject parsedCommand, SocketChannel clientChannel){
        if(!(parsedCommand instanceof Array commands))
            return false;

        List<RedisObject> elements = commands.getElements();

        if(elements == null || elements.isEmpty()) return false;

        if(!(elements.getFirst() instanceof BulkString commandName))
            return false;

        String command = commandName.getValueAsString().toUpperCase();

        if("REPLCONF".equals(command) || "PSYNC".equals(command)){

            System.out.println("Handling replication command");

            if(ConnectionManager.getConnectionType(clientChannel) == ConnectionManager.ConnectionType.CLIENT){
                ConnectionManager.removeConnection(clientChannel);
                ConnectionManager.registerReplicaConnection(clientChannel);
                System.out.println("Client connection upgraded to replica");
            }

            return true;

        }
        return false;
    }


    public CompletableFuture<SocketChannel> connectToMaster(String masterHost, int masterPort, int replicaPort){
        return CompletableFuture.supplyAsync(() -> {
            System.out.println("Initiating connection to master: " + masterHost + ":" + masterPort);
            try {
                SocketChannel socketChannel = SocketChannel.open();
                socketChannel.connect(new InetSocketAddress(masterHost, masterPort));

                // Perform Handshake
                performHandshake(socketChannel, replicaPort);

                ConnectionManager.setMasterConnection(socketChannel);
                socketChannel.configureBlocking(false);

                System.out.printf("Successfully connected to master %s:%d%n", masterHost, masterPort);
                return socketChannel;

            } catch (Exception e) {
                System.err.println("Failed to connect to master: " + e.getMessage());
                throw new RuntimeException("Master connection failed", e);
            }
        });
    }

    private void performHandshake(SocketChannel socketChannel, int replicaPort) throws IOException {
        sendPingToMaster(socketChannel);

        sendReplConf(socketChannel, "listening-port", String.valueOf(replicaPort));
        sendReplConf(socketChannel, "capa", "psync2");

        sendPsync(socketChannel);
    }


    private void sendReplConf(SocketChannel socketChannel, String key, String value) throws IOException {
        System.out.println("Sending REPLCONF " + key + " " + value);

        List<RedisObject> messageObjects = Arrays.asList(
                new BulkString("REPLCONF".getBytes()),
                new BulkString(key.getBytes()),
                new BulkString(value.getBytes())
        );

        Array replConfCommand = new Array(messageObjects);
        String serialized = RedisSerializer.serialize(replConfCommand);

        sendCommand(socketChannel, serialized);
        String response = receiveResponse(socketChannel);

        if (!response.contains("OK")) {
            throw new IOException("Unexpected REPLCONF response: " + response);
        }

        System.out.println("REPLCONF " + key + " successful");
    }

    private void sendPsync(SocketChannel socketChannel) throws IOException {
        System.out.println("Sending PSYNC ? -1");

        List<RedisObject> messageObjects = Arrays.asList(
                new BulkString("PSYNC".getBytes()),
                new BulkString("?".getBytes()),
                new BulkString("-1".getBytes())
        );

        Array psyncCommand = new Array(messageObjects);
        String serialized = RedisSerializer.serialize(psyncCommand);

        sendCommand(socketChannel, serialized);
        String response = receiveResponse(socketChannel);

        if (!response.contains("FULLRESYNC")) {
            throw new IOException("Unexpected PSYNC response: " + response);
        }

        System.out.println("PSYNC successful, received FULLRESYNC response");
    }

    private void sendPingToMaster(SocketChannel socketChannel) throws IOException {
        System.out.println("Sending PING to master");

        Array pingCommand = new Array(List.of(new BulkString("PING".getBytes())));
        String serialized = RedisSerializer.serialize(pingCommand);

        sendCommand(socketChannel, serialized);
        String response = receiveResponse(socketChannel);

        if (!response.contains("PONG")) {
            throw new IOException("Unexpected PING response: " + response);
        }

        System.out.println("PING successful");
    }

    public void handleMasterCommand(RedisObject command){
        try{
            System.out.println("Processing command from master : "+ extractCommandName(command));

            RedisCommandHandler.executeCommand(command);
            System.out.println("Master command executed successfully");

        } catch (Exception e) {
            System.err.println("Error executing master command : "+ e.getMessage());
        }
    }


    private String receiveResponse(SocketChannel channel) throws IOException {
        ByteBuffer responseBuffer = ByteBuffer.allocate(10240);
        long startTime = System.currentTimeMillis();
        int totalBytesRead = 0;
        while (totalBytesRead == 0 && System.currentTimeMillis() - startTime <= HANDSHAKE_TIMEOUT_MS){
            int bytesRead = channel.read(responseBuffer);
            if(bytesRead > 0){
                totalBytesRead += bytesRead;
            } else if (bytesRead == 0) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for response");
                }
            } else {
                throw new IOException("Connection closed while reading response");
            }
        }

        if(totalBytesRead == 0){
            throw new IOException("Timeout waiting for master");
        }

        String response = new String(responseBuffer.array(),0 , totalBytesRead);
        System.out.println("Received response: " + response.replace("\r", "\\r").replace("\n", "\\n"));

        responseBuffer.clear();
        return response;

    }

    private String extractCommandName(RedisObject parsedCommand) {
        if (!(parsedCommand instanceof Array commands)) {
            return "NOT_ARRAY";
        }

        List<RedisObject> elements = commands.getElements();
        if (elements == null || elements.isEmpty()) {
            return "EMPTY_ARRAY";
        }

        if (!(elements.getFirst() instanceof BulkString firstCommand)) {
            return "NOT_BULK_STRING";
        }

        return firstCommand.getValueAsString().toUpperCase();
    }

    public boolean hasActiveMasterConnection() {
        SocketChannel masterChannel = ConnectionManager.getMasterConnection();
        return masterChannel != null && masterChannel.isOpen() && masterChannel.isConnected();
    }


    private void sendCommand(SocketChannel socketChannel, String command) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(command.getBytes());

        while (buffer.hasRemaining()) {
            int written = socketChannel.write(buffer);
            if (written == 0) {
                // Handle potential blocking
                Thread.yield();
            }
        }
    }

}
