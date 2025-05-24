public class CommandParser {
    private int masterPort;
    private int port;
    private boolean isMaster;
    private boolean isReplica;
    private String masterHost;
    public CommandParser(String[] args){
        parseArgs(args);
    }

    private void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            if(arg.contains("--port")){
                if(i+1 < args.length){
                    try {
                        port = Integer.parseInt(args[i+1]);
                    }catch (Exception e){
                        throw new IllegalArgumentException("Invalid number");
                    }
                    i++;
                }else {
                    throw new IllegalArgumentException("--port should be followed by a port number");
                }
            }

            else if(arg.contains("--replicaof")){

                if(i+1 < args.length){
                    parseMasterInfo(args[i+1]);
                    i++;
                    this.isReplica = true;
                }else {
                    throw new IllegalArgumentException("--port should be followed by a port number");
                }

            }
        }
    }

    private void parseMasterInfo(String arg) {
        String[] parts = arg.split(" ");
        try {
            masterHost = parts[0];
            masterPort = Integer.parseInt(parts[1]);
        }catch (NumberFormatException e){
            throw new IllegalArgumentException("Master port is not specified correctly");
        }
    }

    public int getMasterPort() {
        return masterPort;
    }

    public void setMasterPort(int masterPort) {
        this.masterPort = masterPort;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public boolean isMaster() {
        return isMaster;
    }

    public void setMaster(boolean master) {
        isMaster = master;
    }

    public boolean isReplica() {
        return isReplica;
    }

    public void setReplica(boolean replica) {
        isReplica = replica;
    }

    public String getMasterHost() {
        return masterHost;
    }

    public void setMasterHost(String masterHost) {
        this.masterHost = masterHost;
    }
}
