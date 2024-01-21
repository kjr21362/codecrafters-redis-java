import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 6379;
        try {
          serverSocket = new ServerSocket(port);
          serverSocket.setReuseAddress(true);
          // Wait for connection from client.
          clientSocket = serverSocket.accept();

          if (clientSocket != null) {
              try(BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                  PrintWriter printWriter = new PrintWriter(clientSocket.getOutputStream(), true)) {
                  String clientCommand;
                  while ((clientCommand = bufferedReader.readLine()) != null) {
                      if (clientCommand.equals("ping")) {
                      //System.out.println("clientCommand: " + clientCommand);
                          printWriter.print("+PONG\r\n");
                          printWriter.flush();
                      }
                  }
              }
          }
        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        } finally {
          try {
            if (clientSocket != null) {
              clientSocket.close();
            }
          } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
          }
        }
  }
}
