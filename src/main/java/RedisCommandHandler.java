import objects.Array;
import objects.BulkString;
import objects.RedisObject;
import objects.SimpleString;

import java.io.IOException;
import java.util.HexFormat;
import java.util.List;

public class RedisCommandHandler {
    public static final String EMPTY_RDB = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";


    public static CommandResponse executeCommand(RedisObject parsedCommand) throws IOException {
        if(!(parsedCommand instanceof Array commands)) throw new IOException("Command should be an array");

        List<RedisObject> redisObjects = commands.getElements();

        if(redisObjects == null || redisObjects.isEmpty()) throw new IOException("Empty command");

        if(!(redisObjects.get(0) instanceof BulkString commandName)) throw new IOException("First input should be a command");

        String command = commandName.getValueAsString().toUpperCase();

        return switch (command) {
            case "PING" -> new CommandResponse(RedisSerializer.serialize(new SimpleString("PONG")));
            case "ECHO" -> {
                if (redisObjects.size() < 2)
                    throw new IOException("ECHO requires an argument");
                yield new CommandResponse().addRespObject(redisObjects.get(1));
            }
            case "SET" -> new CommandResponse().addRespObject(handleSet(redisObjects));
            case "GET" -> new CommandResponse().addRespObject(handleGet(redisObjects));
            case "INFO" -> new CommandResponse().addRespObject(handleInfo(redisObjects));
            case "REPLCONF" -> new CommandResponse().addRespObject(handleReplConf(redisObjects));
            case "PSYNC" -> handlePsync(redisObjects);
            default -> throw new IOException("Unsupported command");
        };

    }

    /**
     * Enhanced PSYNC handler with detailed debugging to track exactly what we're sending
     */
    private static CommandResponse handlePsync(List<RedisObject> redisObjects) {
        String value = ((BulkString) redisObjects.getFirst()).getValueAsString();

        System.out.println("=== PSYNC Handler Called ===");
        System.out.println("Command: " + value);

        if (!value.equalsIgnoreCase("psync")) {
            System.out.println("Not a PSYNC command, returning empty response");
            return new CommandResponse(); // Empty response
        }

        if (!RedisServerState.isLeader()) {
            System.out.println("Server is not leader, returning null response");
            return new CommandResponse().addRespObject(new BulkString(null));
        }

        System.out.println("Server is leader, preparing FULLRESYNC response");

        // Create the multi-part response for PSYNC
        CommandResponse response = new CommandResponse();

        // Part 1: Send the FULLRESYNC response
        String fullResyncResponse = String.format(
                "+FULLRESYNC %s %d\r\n",
                "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
                0
        );
        System.out.println("Part 1 - FULLRESYNC response: " + fullResyncResponse.replace("\r\n", "\\r\\n"));
        response.addStringObject(fullResyncResponse);

        // Part 2: Send the RDB file as binary data
        // Convert hex string to actual bytes for the empty RDB
        byte[] rdbBytes = HexFormat.of().parseHex(EMPTY_RDB);
        System.out.println("RDB data parsed: " + rdbBytes.length + " bytes");

        // The RDB file should be sent as a bulk string: $<length>\r\n<data>
        String rdbSizePrefix = String.format("$%d\r\n", rdbBytes.length);
        System.out.println("Part 2 - RDB size prefix: " + rdbSizePrefix.replace("\r\n", "\\r\\n"));
        response.addStringObject(rdbSizePrefix);

        // Part 3: The actual RDB content (this should NOT have \r\n at the end)
        System.out.println("Part 3 - RDB binary data: " + rdbBytes.length + " bytes of binary content");
        response.addBinaryObject(rdbBytes);

        System.out.println("PSYNC response prepared with " + response.getParts().size() + " parts");
        System.out.println("=== PSYNC Handler Complete ===");
        response.setMultiPart();

        return response;
    }

    private static RedisObject handleReplConf(List<RedisObject> redisObjects) {
        String value = ((BulkString) redisObjects.getFirst()).getValueAsString();
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

        if(!(redisObjects.get(1) instanceof BulkString key)){
            throw new IOException("Key should be a string");
        }
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
