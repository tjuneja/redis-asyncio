package v2;

import objects.RedisObject;
import objects.SimpleString;
import objects.Error;
import objects.Integer;
import objects.BulkString;
import objects.Array;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RespParser {

    /**
     * Results of parsing operation
     */

    public static class ParseResult{
        private final RedisObject parsedObject;
        private final boolean complete;
        private final int consumedBytes;
        private final String remainingData;
        private final ParseError error;

        private ParseResult(RedisObject redisObject, int consumedBytes, String remainingData) {
            this.parsedObject = redisObject;
            this.complete = true;
            this.consumedBytes = consumedBytes;
            this.remainingData = remainingData;
            this.error = null;
        }
        private ParseResult(int consumedBytes, String remainingData,ParseError error ){
            this.parsedObject = null;
            this.complete = false;
            this.consumedBytes = consumedBytes;
            this.remainingData = remainingData;
            this.error = error;
        }

        public RedisObject getParsedObject() {
            return parsedObject;
        }


        public static ParseResult success(RedisObject redisObject, int consumedBytes, String remainingData){
            return new ParseResult(redisObject, consumedBytes, remainingData);
        }

        public static ParseResult incomplete(String remainingData){
            return new ParseResult(0, remainingData, ParseError.INCOMPLETE_DATA);
        }

        public static ParseResult error(int consumedBytes, String remainingData, ParseError error){
            return new ParseResult(consumedBytes, remainingData, error);
        }

        public boolean isSuccess(){
            return complete && error == null;
        }

        public boolean isComplete() {
            return complete;
        }

        public int getConsumedBytes() {
            return consumedBytes;
        }

        public String getRemainingData() {
            return remainingData;
        }

        public ParseError getError() {
            return error;
        }
    }


    public enum ParseError {
        INCOMPLETE_DATA("Incomplete RESP data"),
        INVALID_TYPE("Invalid RESP data type"),
        INVALID_LENGTH("Invalid length specification"),
        MALFORMED_DATA("Malformed RESP data"),
        UNEXPECTED_EOF("Unexpected end of data");

        private final String description;

        ParseError(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }


    public static RedisObject parse(String input) throws IOException {
        ParseResult result = parseStreaming(input);

        if(!result.isSuccess()){
            throw new IOException("Parse error: " +
                    (result.error != null ? result.error.getDescription() : "Unknown error"));
        }

        return result.getParsedObject();
    }

    private static ParseResult parseStreaming(String data) {
        if(data == null || data.isEmpty()){
            return ParseResult.incomplete(data);
        }
        try {
            return parseRespObject(data,0);
        } catch (Exception e) {
            return ParseResult.error(0, data,ParseError.MALFORMED_DATA);
        }
    }

    private static ParseResult parseRespObject(String data, int startPos) {
        if (startPos >= data.length()) {
            return ParseResult.incomplete(data.substring(startPos));
        }

        char type = data.charAt(startPos);

        switch (type) {
            case '+': // Simple String
                return parseSimpleString(data, startPos);
            case '-': // Error
                return parseError(data, startPos);
            case ':': // Integer
                return parseInteger(data, startPos);
            case '$': // Bulk String
                return parseBulkString(data, startPos);
            case '*': // Array
                return parseArray(data, startPos);
            default:
                return ParseResult.error(startPos, data.substring(startPos),ParseError.INVALID_TYPE);
        }
    }

    private static ParseResult parseSimpleString(String data, int startPos) {
        int crlfPos = findCRLF(data, startPos + 1);
        if (crlfPos == -1) {
            return ParseResult.incomplete(data.substring(startPos));
        }

        String value = data.substring(startPos + 1, crlfPos);
        RedisObject object = new SimpleString(value);
        String remaining = data.substring(crlfPos + 2);

        return ParseResult.success(object, crlfPos + 2 - startPos, remaining);
    }
    private static ParseResult parseError(String data, int startPos) {
        int crlfPos = findCRLF(data, startPos + 1);
        if (crlfPos == -1) {
            return ParseResult.incomplete(data.substring(startPos));
        }

        String value = data.substring(startPos + 1, crlfPos);
        RedisObject object = new Error(value);
        String remaining = data.substring(crlfPos + 2);

        return ParseResult.success(object, crlfPos + 2 - startPos, remaining);
    }

    private static ParseResult parseInteger(String data, int startPos) {
        int crlfPos = findCRLF(data, startPos + 1);
        if (crlfPos == -1) {
            return ParseResult.incomplete(data.substring(startPos));
        }

        try {
            String value = data.substring(startPos + 1, crlfPos);
            long intValue = Long.parseLong(value);
            RedisObject object = new Integer(intValue);
            String remaining = data.substring(crlfPos + 2);

            return ParseResult.success(object, crlfPos + 2 - startPos, remaining);
        } catch (NumberFormatException e) {
            return ParseResult.error(startPos, data.substring(startPos),ParseError.INVALID_LENGTH);
        }
    }

    private static ParseResult parseBulkString(String data, int startPos) {
        int crlfPos = findCRLF(data, startPos + 1);
        if (crlfPos == -1) {
            return ParseResult.incomplete(data.substring(startPos));
        }

        try {
            String lengthStr = data.substring(startPos + 1, crlfPos);
            int length = java.lang.Integer.parseInt(lengthStr);

            if (length == -1) {
                // Null bulk string
                RedisObject object = new BulkString(null);
                String remaining = data.substring(crlfPos + 2);
                return ParseResult.success(object, crlfPos + 2 - startPos, remaining);
            }

            if (length < 0) {
                return ParseResult.error(startPos, data.substring(startPos), ParseError.INVALID_LENGTH);
            }

            int dataStart = crlfPos + 2;
            int dataEnd = dataStart + length;
            int expectedEnd = dataEnd + 2; // Including final CRLF

            if (expectedEnd > data.length()) {
                return ParseResult.incomplete(data.substring(startPos));
            }

            byte[] value = data.substring(dataStart, dataEnd).getBytes(StandardCharsets.UTF_8);
            RedisObject object = new BulkString(value);
            String remaining = data.substring(expectedEnd);

            return ParseResult.success(object, expectedEnd - startPos, remaining);

        } catch (NumberFormatException e) {
            return ParseResult.error( startPos, data.substring(startPos), ParseError.INVALID_LENGTH);
        }
    }
    private static ParseResult parseArray(String data, int startPos) {
        int crlfPos = findCRLF(data, startPos + 1);
        if (crlfPos == -1) {
            return ParseResult.incomplete(data.substring(startPos));
        }

        try {
            String lengthStr = data.substring(startPos + 1, crlfPos);
            int length = java.lang.Integer.parseInt(lengthStr);

            if (length == -1) {
                // Null array
                RedisObject object = new Array(null);
                String remaining = data.substring(crlfPos + 2);
                return ParseResult.success(object, crlfPos + 2 - startPos, remaining);
            }

            if (length < 0) {
                return ParseResult.error(startPos, data.substring(startPos),ParseError.INVALID_LENGTH);
            }

            List<RedisObject> elements = new ArrayList<>(length);
            int currentPos = crlfPos + 2;
            int totalConsumed = currentPos - startPos;

            for (int i = 0; i < length; i++) {
                ParseResult elementResult = parseRespObject(data, currentPos);

                if (!elementResult.isComplete()) {
                    return ParseResult.incomplete(data.substring(startPos));
                }

                elements.add(elementResult.getParsedObject());
                currentPos += elementResult.getConsumedBytes();
                totalConsumed += elementResult.getConsumedBytes();
            }

            RedisObject object = new Array(elements);
            String remaining = data.substring(currentPos);

            return ParseResult.success(object, totalConsumed, remaining);

        } catch (NumberFormatException e) {
            return ParseResult.error( startPos, data.substring(startPos),ParseError.INVALID_LENGTH);
        }
    }
    private static int findCRLF(String data, int startPos){
        for (int i = startPos; i < data.length()-1; i++) {
            if(data.charAt(i) == '\r' && data.charAt(i+1) =='\n'){
                return i;
            }
        }
        return -1;
    }

    private static int findArrayPosition(String data){
        for (int i = 0; i < data.length(); i++) {
            if(data.charAt(i) == '*'){
                return i;
            }
        }

        return data.length();
    }

    public static List<ParseResult> parseMultiple(String accumulatedData) {
        List<ParseResult> results = new ArrayList<>();
        String remainingData = accumulatedData;

        while (!remainingData.isEmpty()) {
            // Skip non-command data (handshake responses, etc.)
            int arrayStart = findArrayPosition(remainingData);
            if (arrayStart > 0) {
                remainingData = remainingData.substring(arrayStart);
                continue;
            }

            if (arrayStart == remainingData.length()) {
                // No more arrays found
                break;
            }

            ParseResult result = parseStreaming(remainingData);
            results.add(result);

            if (!result.isComplete()) {
                // Incomplete data, stop parsing
                break;
            }

            remainingData = result.getRemainingData();
        }

        return results;
    }
}
