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


    public static void propagateCommand(RedisObject parsedCommand){
        if(!RedisServerState.isLeader() || !ConnectionManager.hasReplicas()) return;

        if(!canPropagateCommand(parsedCommand)) {
            System.out.println("Not a command that can be propagated");
        }

        String serializedCommand = RedisSerializer.serialize(parsedCommand);
        byte[] commandBytes = serializedCommand.getBytes();

        System.out.println("Propagating command " + serializedCommand);

        List<SocketChannel> replicas = ConnectionManager.getReplicaConnections();

        for (SocketChannel channel: replicas){
            propagateCommandToReplica(channel, commandBytes);
        }

    }

    private static boolean canPropagateCommand(RedisObject parsedCommand) {

        if(! (parsedCommand instanceof Array commands)){
            return false;
        }

        List<RedisObject> elements = commands.getElements();

        if(!(elements.getFirst() instanceof BulkString firstCommand)){
            return false;
        }
        String commandName = firstCommand.getValueAsString().toUpperCase();
        if(NON_PROPAGATED_COMMANDS.contains(commandName)) return false;

        return WRITE_COMMANDS.contains(commandName);
    }

    private static void propagateCommandToReplica(SocketChannel channel, byte[] commandBytes) {
        CompletableFuture.runAsync(() ->{
           try{
               ByteBuffer buffer = ByteBuffer.wrap(commandBytes);
               int bytesWritten = 0;
               while (buffer.hasRemaining()){
                   int written = channel.write(buffer);
                   bytesWritten+= written;

                   if(written == 0){
                       Thread.yield();
                   }
               }
               System.out.println("Successfully sent " + bytesWritten + " bytes to replica: " + channel);


           } catch (Exception e) {
               System.out.println("Failed to replicate command to replica : "+ channel);
               ConnectionManager.removeConnection(channel);

               try {
                   channel.close();
               } catch (IOException closeException) {
                   System.err.println("Failed to close failed replica connection: " + closeException.getMessage());
               }

           }
        });
    }


}
