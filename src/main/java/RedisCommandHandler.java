import objects.Array;
import objects.BulkString;
import objects.RedisObject;
import objects.SimpleString;

import java.io.IOException;
import java.util.List;

public class RedisCommandHandler {
    public static RedisObject executeCommand(RedisObject parsedCommand) throws IOException {
        if(!(parsedCommand instanceof Array commands)) throw new IOException("Command should be an array");

        List<RedisObject> redisObjects = commands.getElements();

        if(redisObjects == null || redisObjects.isEmpty()) throw new IOException("Empty command");

        if(!(redisObjects.get(0) instanceof BulkString commandName)) throw new IOException("First input should be a command");

        String command = commandName.getValueAsString().toUpperCase();

        return switch (command) {
            case "PING" -> new SimpleString("PONG");
            case "ECHO" -> {
                if (redisObjects.size() < 2)
                    throw new IOException("ECHO requires an argument");
                yield redisObjects.get(1);
            }
            case "SET" -> handleSet(redisObjects);
            case "GET" -> handleGet(redisObjects);
            case "INFO" -> handleInfo(redisObjects);
            case "REPLCONF" -> handleReplConf(redisObjects);
            case "PSYNC" -> handlePsync(redisObjects);
            default -> throw new IOException("Unsupported command");
        };

    }

    private static RedisObject handlePsync(List<RedisObject> redisObjects) {
        String value = ((BulkString) redisObjects.get(0)).getValueAsString();
        if (value.equalsIgnoreCase("psync")) {
            if (RedisServerState.isLeader()) {
                StringBuilder sb = new StringBuilder()
                        .append("FULLRESYNC ")
                        .append("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb ")
                        .append("0");
                return new BulkString(sb.toString().getBytes());
            } else {
                return new BulkString(null);
            }
        }
        else return null;
    }

    private static RedisObject handleReplConf(List<RedisObject> redisObjects) {
        String value = ((BulkString) redisObjects.get(0)).getValueAsString();
        if(value.equalsIgnoreCase("replconf")){
            return new BulkString("OK".getBytes());
        }else{
            return new BulkString(null);
        }
    }

    private static RedisObject handleInfo(List<RedisObject> redisObjects) throws IOException {
        String value = ((BulkString) redisObjects.get(1)).getValueAsString();
        if(value.equalsIgnoreCase("replication")){
            if(RedisServerState.isLeader()){
                StringBuilder sb = new StringBuilder().append(RedisServerState.getStatus()).append("\n")
                        .append("master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb")
                        .append("master_repl_offset:0");
                return new BulkString(sb.toString().getBytes());
            }else
                return new BulkString(RedisServerState.getStatus().getBytes());
        }else{
            return new BulkString(null);
        }
    }

    private static RedisObject handleGet(List<RedisObject> redisObjects) throws IOException {
//        if(redisObjects.size() != 2) throw new IOException(" Wrong number of arguments");

        if(!(redisObjects.get(1) instanceof BulkString)){
            throw new IOException("Key should be a string");
        }
        BulkString key = (BulkString) redisObjects.get(1);
        byte[] value = RedisStore.get(key.getValueAsString());
        if(value == null)
        {
            System.out.println("Returning null value");
            return new BulkString(null);
        }

        return new BulkString(value);

    }

    private static RedisObject handleSet(List<RedisObject> redisObjects) throws IOException {
        if (redisObjects.size() <3)
            throw new IOException("Wrong number of arguments for set command");

        if(!(redisObjects.get(1) instanceof BulkString) || !(redisObjects.get(2) instanceof BulkString)){
            throw new IOException("ERR key and value must be strings");
        }

        String key = ((BulkString) redisObjects.get(1)).getValueAsString();
        byte[] value = ((BulkString) redisObjects.get(2)).getValue();

        if(redisObjects.size() >3){
            String expiryValue = ((BulkString)redisObjects.get(4)).getValueAsString();

            System.out.println("Expiry value : "+expiryValue);
            RedisStore.set(key,value, Long.parseLong(expiryValue));
        }else{
            RedisStore.set(key, value);
        }
        return new SimpleString("OK");
    }


}
