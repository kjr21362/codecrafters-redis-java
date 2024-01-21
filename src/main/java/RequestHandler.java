import lombok.AllArgsConstructor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

@AllArgsConstructor
public class RequestHandler implements Runnable {

    private Socket clientSocket;
    @Override
    public void run() {
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
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
