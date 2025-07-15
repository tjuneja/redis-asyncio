import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ResponseWriter {

    private static final int DEFAULT_BUFFER_SIZE = 8192;
    private final ConcurrentMap<SocketChannel, PendingResponse> pendingResponses;

    public ResponseWriter(){
        this.pendingResponses = new ConcurrentHashMap<>();
    }

    public void queueResponse(SelectionKey key, CommandResponse response){
        SocketChannel channel = (SocketChannel) key.channel();

        if(!channel.isOpen() || !channel.isConnected()){
            System.err.println("Cannot queue response - Channel is closed or disconnected");
            return;
        }

        if(response == null || (!response.hasResponse() && !response.isMultiPart())){
            System.err.println("No response to write");
            return;
        }

        PendingResponse pendingResponse = createPendingResponse(response);
        pendingResponses.put(channel, pendingResponse);
        key.interestOps(SelectionKey.OP_WRITE);

        System.out.println("Queued response for channel: " + channel);
    }


    public void queueResponses(SelectionKey key, List<CommandResponse> responses){
        SocketChannel channel = (SocketChannel) key.channel();

        if(!channel.isOpen() || !channel.isConnected()){
            System.err.println("Cannot queue response - Channel is closed or disconnected");
            return;
        }
        PendingResponse.Builder builder = new PendingResponse.Builder();
        for (CommandResponse response: responses){

            if(response !=null && (response.hasResponse() || response.isMultiPart()))
                builder.addResponse(response);
        }

        PendingResponse pendingResponse = builder.build();

        if(pendingResponse.hasData()){
            pendingResponses.put(channel, pendingResponse);
            key.interestOps(SelectionKey.OP_WRITE);
            System.out.println("Queued " + responses.size() + " responses for channel: " + channel);
        }
    }


    public WriteResult writeResponse(SelectionKey key) throws IOException{
        SocketChannel channel = (SocketChannel) key.channel();
        PendingResponse pendingResponse = pendingResponses.get(channel);

        if(pendingResponse == null){
            key.interestOps(SelectionKey.OP_READ);
            return WriteResult.completed();
        }

        try{
            boolean completed = writePendingData(channel, pendingResponse);

            if(completed){
                pendingResponses.remove(channel);
                key.interestOps(SelectionKey.OP_READ);
                return WriteResult.completed();
            }else {
                return WriteResult.partial();
            }
        } catch (IOException e) {
            pendingResponses.remove(channel);
            throw e;
        }


    }

    private boolean writePendingData(SocketChannel channel, PendingResponse pendingResponse) throws IOException {
        while(pendingResponse.hasData()){
            ByteBuffer buffer = pendingResponse.getCurrentBuffer();

            if (buffer == null || !buffer.hasRemaining()){
                pendingResponse.moveToNextBuffer();
                continue;
            }

            int bytesWritten = channel.write(buffer);

            if(bytesWritten == 0){
                return false;
            }

            pendingResponse.addBytesWritten(bytesWritten);

            System.out.println("Wrote " + bytesWritten + " bytes to " + channel);

            if (!buffer.hasRemaining()) {
                pendingResponse.moveToNextBuffer();
            }
        }

        System.out.println("Completed writing all data to " + channel);
        return true;
    }
    /**
     * Clean up resources for a disconnected channel
     */
    public void cleanupChannel(SocketChannel channel) {
        PendingResponse removed = pendingResponses.remove(channel);
        if (removed != null) {
            System.out.println("Cleaned up pending response for disconnected channel: " + channel);
        }
    }

    private PendingResponse createPendingResponse(CommandResponse pendingResponse) {
        PendingResponse.Builder builder = new PendingResponse.Builder();
        builder.addResponse(pendingResponse);
        return builder.build();
    }

    public static class WriteResult{
        private final boolean completed;
        private final String status;
        private final int bytesWritten;

        private WriteResult(boolean completed, String status, int bytesWritten){
            this.completed = completed;
            this.status = status;
            this.bytesWritten = bytesWritten;
        }

        public static WriteResult completed(){
            return new WriteResult(true, "complete", 0);
        }

        public static WriteResult partial(){
            return new WriteResult(false, "partial", 0);
        }

        public static WriteResult error(){
            return new WriteResult(false, "error", 0);
        }

        public boolean isCompleted() {
            return completed;
        }

        public String getStatus() {
            return status;
        }

        public int getBytesWritten() {
            return bytesWritten;
        }
    }
    private static class PendingResponse{
        private final List<ByteBuffer> buffers;
        private int currentBufferIndex;
        private long totalBytesWritten;
        private final long totalBytes;

        private PendingResponse(List<ByteBuffer> buffers, long totalBytes) {
            this.buffers = buffers;
            this.currentBufferIndex = 0;
            this.totalBytesWritten = 0;
            this.totalBytes = totalBytes;
        }

        public boolean hasData() {
            return currentBufferIndex < buffers.size();
        }

        public ByteBuffer getCurrentBuffer() {
            if (currentBufferIndex < buffers.size()) {
                return buffers.get(currentBufferIndex);
            }
            return null;
        }

        public void moveToNextBuffer() {
            currentBufferIndex++;
        }

        public void addBytesWritten(int bytes) {
            totalBytesWritten += bytes;
        }

        public long getRemainingBytes() {
            return totalBytes - totalBytesWritten;
        }


        public static class Builder{
            private final List<ByteBuffer> buffers = new ArrayList<>();
            private long totalBytes = 0;


            public Builder addResponse(CommandResponse response){
                if(response.isMultiPart()){
                    for(CommandResponse.ResponsePart part: response.getParts()){
                     ByteBuffer buffer = ByteBuffer.wrap(part.getData());
                     buffers.add(buffer);
                     this.totalBytes += part.getData().length;
                    }
                }else if(response.getStringResponse() != null){
                    byte[] data = response.getStringResponse().getBytes();
                    ByteBuffer buffer = ByteBuffer.wrap(data);
                    buffers.add(buffer);
                    this.totalBytes += data.length;
                }

                return this;
            }

            public PendingResponse build(){
                return new PendingResponse(buffers, this.totalBytes);
            }
        }
    }


}
