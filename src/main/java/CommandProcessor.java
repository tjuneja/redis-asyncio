import connection.ConnectionManager;
import objects.Array;
import objects.BulkString;
import objects.Error;
import objects.RedisObject;
import v2.RespParser;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class CommandProcessor {
    private final ReplicationHandler replicationHandler;

    public CommandProcessor(){
        this.replicationHandler = new ReplicationHandler();
    }

    private boolean isWriteCommand(String commandName) {
        return switch (commandName) {
            case "SET", "DEL", "EXPIRE", "EXPIREAT", "PERSIST", "RENAME", "RENAMENX",
                 "LPUSH", "RPUSH", "LPOP", "RPOP", "LSET", "LTRIM",
                 "SADD", "SREM", "SPOP", "SMOVE",
                 "ZADD", "ZREM", "ZINCRBY", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE",
                 "HSET", "HDEL", "HINCRBY", "HINCRBYFLOAT",
                 "INCR", "DECR", "INCRBY", "DECRBY", "APPEND",
                 "FLUSHDB", "FLUSHALL" -> true;
            default -> false;
        };
    }
    private boolean shouldPropagateCommand(RedisObject parsedCommand) {
        if (!(parsedCommand instanceof Array commands)) {
            return false;
        }

        List<RedisObject> elements = commands.getElements();
        if (elements == null || elements.isEmpty()) {
            return false;
        }

        if (!(elements.getFirst() instanceof BulkString firstCommand)) {
            return false;
        }

        String commandName = firstCommand.getValueAsString().toUpperCase();

        // Define write commands that should be propagated
        return isWriteCommand(commandName) && !isAdminCommand(commandName);
    }


    private boolean isAdminCommand(String commandName) {
        return switch (commandName) {
            case "PING", "ECHO", "INFO", "REPLCONF", "PSYNC", "GET", "EXISTS", "TYPE",
                 "TTL", "PTTL", "KEYS", "SCAN" -> true;
            default -> false;
        };
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


    private CommandResponse createErrorResponse(String errorMessage){
        return new CommandResponse().addRespObject(new Error(errorMessage));
    }

    public CommandProcessingResult processCommands(String accumulatedData, SocketChannel channel) {
        CommandProcessingResult.Builder resultBuilder = new CommandProcessingResult.Builder();

        try {
            List<RespParser.ParseResult> parseResults = RespParser.parseMultiple(accumulatedData);

            for (RespParser.ParseResult parseResult: parseResults){
                if(!parseResult.isComplete()){
                    resultBuilder.setRemainingData(parseResult.getRemainingData());
                    break;
                }

                if(!parseResult.isSuccess()){
                    resultBuilder.addResponse(createErrorResponse(parseResult.getError().getDescription()));
                    resultBuilder.addConsumedBytes(parseResult.getConsumedBytes());
                    break;
                }

                CommandExecutionResult commandExecutionResult = processIndividualCommand(parseResult.getParsedObject(), channel);
                resultBuilder.addResponse(commandExecutionResult.getResponse());
                resultBuilder.addConsumedBytes(parseResult.getConsumedBytes());
                if(commandExecutionResult.shouldPropagate()){
                    CommandPropagator.propagateCommand(parseResult.getParsedObject());
                }

            }

        } catch (Exception e) {

        }

        return resultBuilder.build();
    }

    public CommandExecutionResult processIndividualCommand(RedisObject parsedCommands, SocketChannel channel) {
        try {
            String commandName = extractCommandName(parsedCommands);
            ConnectionManager.ConnectionType connectionType = ConnectionManager.getConnectionType(channel);

            //Handle replication command
            if(replicationHandler.isReplicaHandshake(parsedCommands, channel)){
                CommandResponse response = RedisCommandHandler.executeCommand(parsedCommands);
                return CommandExecutionResult.handshakeResponse(response);
            }

            if(connectionType == ConnectionManager.ConnectionType.MASTER && isReplConfGetAck(parsedCommands)){
                CommandResponse response = RedisCommandHandler.executeCommand(parsedCommands);
                return CommandExecutionResult.replConfGetAckResponse(response);
            }

            if (connectionType == ConnectionManager.ConnectionType.MASTER){
                RedisCommandHandler.executeCommand(parsedCommands);
                return CommandExecutionResult.silentExecution();
            }

            CommandResponse response = RedisCommandHandler.executeCommand(parsedCommands);
            boolean shouldPropagate = shouldPropagateCommand(parsedCommands);
            return CommandExecutionResult.clientResponse(response, shouldPropagate);
        }catch (Exception e){
            CommandResponse response = new CommandResponse().addRespObject(new Error("Err"+ e.getMessage()));
            return CommandExecutionResult.errorResponse(response);
        }
    }

    private boolean isReplConfGetAck(RedisObject parsedCommands) {
        if(!(parsedCommands instanceof Array)) return false;

        List<RedisObject> commands = ((Array)parsedCommands).getElements();

        if(commands.isEmpty() || commands.size() < 2) return false;

        String firstCommand = ((BulkString)commands.getFirst()).getValueAsString();
        String secondCommand = ((BulkString)commands.get(1)).getValueAsString();

        return "REPLCONF".equalsIgnoreCase(firstCommand) && "GETACK".equalsIgnoreCase(secondCommand);
    }


    public static class CommandExecutionResult{
        private final CommandResponse response;
        private final boolean shouldPropogate;
        private final boolean shouldRespond;
        private ExecutionType type;

        public enum ExecutionType{
            CLIENT_RESPONSE,
            HANDSHAKE_RESPONSE,
            SILENT_EXECUTION,
            ERROR_RESPONSE,
            MASTER_GETACK_RESPONSE
        }

        private CommandExecutionResult(CommandResponse response, boolean shouldPropagate,
                                       boolean shouldRespond, ExecutionType type) {
            this.response = response;
            this.shouldPropogate = shouldPropagate;
            this.shouldRespond = shouldRespond;
            this.type = type;
        }

        public static CommandExecutionResult clientResponse(CommandResponse response, boolean shouldPropagate) {
            return new CommandExecutionResult(response, shouldPropagate, true, ExecutionType.CLIENT_RESPONSE);
        }

        public static CommandExecutionResult handshakeResponse(CommandResponse response) {
            return new CommandExecutionResult(response, false, true, ExecutionType.HANDSHAKE_RESPONSE);
        }

        public static CommandExecutionResult silentExecution() {
            return new CommandExecutionResult(null, false, false, ExecutionType.SILENT_EXECUTION);
        }

        public static CommandExecutionResult errorResponse(CommandResponse response) {
            return new CommandExecutionResult(response, false, true, ExecutionType.ERROR_RESPONSE);
        }

        public static CommandExecutionResult replConfGetAckResponse(CommandResponse response) {
            return new CommandExecutionResult(response, false, true, ExecutionType.MASTER_GETACK_RESPONSE);
        }

        // Getters
        public CommandResponse getResponse() { return response; }
        public boolean shouldPropagate() { return shouldPropogate; }
        public boolean shouldRespond() { return shouldRespond; }
        public ExecutionType getType() { return type; }

    }


    public record CommandProcessingResult(List<CommandResponse> responses, int totalConsumedBytes, String remainingData,
                                          boolean hasErrors) {

        public boolean hasResponse() {
                return (responses != null && !responses.isEmpty());
            }

            public static class Builder {
                private final List<CommandResponse> responses = new ArrayList<>();
                private int totalConsumedBytes = 0;
                private String remainingData = "";
                private boolean hasErrors = false;

                public Builder addResponse(CommandResponse response) {
                    if (response != null) {
                        responses.add(response);
                    }
                    return this;
                }

                public Builder addConsumedBytes(int bytes) {
                    totalConsumedBytes += bytes;
                    return this;
                }

                public Builder setRemainingData(String data) {
                    this.remainingData = data != null ? data : "";
                    return this;
                }

                public Builder setHasErrors(boolean hasErrors) {
                    this.hasErrors = hasErrors;
                    return this;
                }

                public CommandProcessingResult build() {
                    return new CommandProcessingResult(responses, totalConsumedBytes, remainingData, hasErrors);
                }
            }


        }


}
