import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args){
        // You can use print statements as follows for debugging, they'll be visible when running tests.
        System.out.println("Logs from your program will appear here!");
        ExecutorService executor = Executors.newFixedThreadPool(10);
        //  Uncomment this block to pass the first stage
        ServerSocket serverSocket = null;

        int port = 6379;
        try {
            serverSocket = new ServerSocket(port);
            // Since the tester restarts your program quite often, setting SO_REUSEADDR
            // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);
            while(true) {
                final Socket clientSocket = serverSocket.accept();

                executor.submit(() ->{
                    try{
                        handleClients(clientSocket);
                    }finally {
                        try {
                            clientSocket.close();
                        } catch (IOException e) {
                            System.out.println("IOException \n"+ e.getMessage());
                        }
                    }
                });

            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (serverSocket != null && !serverSocket.isClosed()) {
                    serverSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }

    private static void handleClients(Socket clientSocket){
        try {
            InputStream in = clientSocket.getInputStream();
            OutputStream out = clientSocket.getOutputStream();
            byte[] buffer  = new byte[1024];
            int bytesRead ;
            
            while((bytesRead = in.read(buffer)) != -1){

                String input = new String(buffer,0, bytesRead);
                out.write("+PONG\r\n".getBytes());
                out.flush();
            }
        } catch (IOException e) {
            System.out.println("Error : "+ e.getMessage());
        }

    }
}
