import connection.ConnectionManager;
import objects.Array;
import objects.BulkString;
import objects.RedisObject;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class CommandPropagator {
    // Commands that modify server state and should be propagated
    private static final Set<String> WRITE_COMMANDS = new HashSet<>(Arrays.asList(
            "SET", "DEL"
//            "EXPIRE", "EXPIREAT", "PERSIST", "RENAME", "RENAMENX",
//            "LPUSH", "RPUSH", "LPOP", "RPOP", "LSET", "LTRIM",
//            "SADD", "SREM", "SPOP", "SMOVE",
//            "ZADD", "ZREM", "ZINCRBY", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE",
//            "HSET", "HDEL", "HINCRBY", "HINCRBYFLOAT",
//            "INCR", "DECR", "INCRBY", "DECRBY", "APPEND",
//            "FLUSHDB", "FLUSHALL"
            // Add more write commands as you implement them
    ));

    // Commands that are administrative and shouldn't be propagated
    private static final Set<String> NON_PROPAGATED_COMMANDS = new HashSet<>(Arrays.asList(
            "PING", "ECHO", "INFO", "REPLCONF", "PSYNC", "GET", "EXISTS", "TYPE"
//            "TTL", "PTTL", "KEYS", "SCAN", "LLEN", "LINDEX", "LRANGE",
//            "SCARD", "SMEMBERS", "SISMEMBER", "ZCARD", "ZSCORE", "ZRANGE",
//            "HGET", "HGETALL", "HKEYS", "HVALS", "HLEN", "HEXISTS"
    ));


    public static void propagateCommand(RedisObject parsedCommand) {
        System.out.println("=== PROPAGATION DEBUG START ===");

        // Debug: Show what command we're examining
        String commandName = extractCommandName(parsedCommand);
        System.out.println("Examining command for propagation: " + commandName);

        // Check basic preconditions
        if (!RedisServerState.isLeader()) {
            System.out.println("Not leader - no propagation");
            System.out.println("=== PROPAGATION DEBUG END ===");
            return;
        }

        if (!ConnectionManager.hasReplicas()) {
            System.out.println("No replicas connected - no propagation");
            System.out.println("Current replica count: " + ConnectionManager.getReplicaConnections().size());
            System.out.println("=== PROPAGATION DEBUG END ===");
            return;
        }

        System.out.println("Master status: ✓ Leader with " + ConnectionManager.getReplicaConnections().size() + " replicas");

        // Check if command should be propagated
        boolean shouldPropagate = canPropagateCommand(parsedCommand);
        System.out.println("Command '" + commandName + "' should propagate: " + shouldPropagate);

        if (!shouldPropagate) {
            System.out.println("Command not eligible for propagation - skipping");
            System.out.println("=== PROPAGATION DEBUG END ===");
            return;
        }

        // Command should be propagated - proceed
        String serializedCommand = RedisSerializer.serialize(parsedCommand);
        byte[] commandBytes = serializedCommand.getBytes();

        System.out.println("✓ PROPAGATING command: " + commandName);
        System.out.println("✓ Serialized form: " + serializedCommand.trim());
        System.out.println("✓ Byte length: " + commandBytes.length);

        List<SocketChannel> replicas = ConnectionManager.getReplicaConnections();
        System.out.println("✓ Sending to " + replicas.size() + " replica(s)");

        for (int i = 0; i < replicas.size(); i++) {
            SocketChannel replica = replicas.get(i);
            System.out.println("  -> Replica " + (i + 1) + ": " + replica);
            propagateCommandToReplica(replica, commandBytes, commandName);
        }

        System.out.println("=== PROPAGATION COMPLETE ===");
    }

    private static boolean canPropagateCommand(RedisObject parsedCommand) {
        if (!(parsedCommand instanceof Array commands)) {
            System.out.println("  Command is not an Array - cannot propagate");
            return false;
        }

        List<RedisObject> elements = commands.getElements();
        if (elements == null || elements.isEmpty()) {
            System.out.println("  Command array is empty - cannot propagate");
            return false;
        }

        if (!(elements.getFirst() instanceof BulkString firstCommand)) {
            System.out.println("  First element is not BulkString - cannot propagate");
            return false;
        }

        String commandName = firstCommand.getValueAsString().toUpperCase();
        System.out.println("  Checking command: " + commandName);

        if (NON_PROPAGATED_COMMANDS.contains(commandName)) {
            System.out.println("  Command '" + commandName + "' is in NON_PROPAGATED list");
            return false;
        }

        boolean isWriteCommand = WRITE_COMMANDS.contains(commandName);
        System.out.println("  Command '" + commandName + "' is write command: " + isWriteCommand);
        return isWriteCommand;
    }

    private static void propagateCommandToReplica(SocketChannel channel, byte[] commandBytes, String commandName) {
        CompletableFuture.runAsync(() -> {
            try {
                System.out.println("    [ASYNC] Starting propagation of '" + commandName + "' to " + channel);

                ByteBuffer buffer = ByteBuffer.wrap(commandBytes);
                int totalBytes = commandBytes.length;
                int bytesWritten = 0;
                int writeAttempts = 0;

                while (buffer.hasRemaining()) {
                    int written = channel.write(buffer);
                    bytesWritten += written;
                    writeAttempts++;

                    if (written == 0) {
                        System.out.println("    [ASYNC] Write attempt " + writeAttempts + " - yielding");
                        Thread.yield();
                    } else {
                        System.out.println("    [ASYNC] Write attempt " + writeAttempts + " - wrote " + written + " bytes");
                    }
                }

                System.out.println("    [ASYNC] ✓ Successfully sent '" + commandName + "' (" + bytesWritten + "/" + totalBytes + " bytes) to " + channel + " in " + writeAttempts + " attempts");

            } catch (Exception e) {
                System.err.println("    [ASYNC] ✗ Failed to propagate '" + commandName + "' to " + channel + ": " + e.getMessage());
                ConnectionManager.removeConnection(channel);

                try {
                    channel.close();
                } catch (IOException closeException) {
                    System.err.println("    [ASYNC] Failed to close failed replica connection: " + closeException.getMessage());
                }
            }
        });
    }

    /**
     * Helper method to extract command name for debugging
     */
    private static String extractCommandName(RedisObject parsedCommand) {
        if (!(parsedCommand instanceof Array commands)) {
            return "NOT_ARRAY";
        }

        List<RedisObject> elements = commands.getElements();
        if (elements == null || elements.isEmpty()) {
            return "EMPTY_ARRAY";
        }

        if (!(elements.get(0) instanceof BulkString firstCommand)) {
            return "NOT_BULK_STRING";
        }

        return firstCommand.getValueAsString().toUpperCase();
    }

    /**
     * Debug method to show current propagation state
     */
    public static void debugPropagationState() {
        System.out.println("=== PROPAGATION STATE DEBUG ===");
        System.out.println("Server is leader: " + RedisServerState.isLeader());
        System.out.println("Has replicas: " + ConnectionManager.hasReplicas());
        System.out.println("Replica count: " + ConnectionManager.getReplicaConnections().size());

        List<SocketChannel> replicas = ConnectionManager.getReplicaConnections();
        for (int i = 0; i < replicas.size(); i++) {
            System.out.println("  Replica " + (i + 1) + ": " + replicas.get(i));
        }
        System.out.println("=== END STATE DEBUG ===");
    }
}