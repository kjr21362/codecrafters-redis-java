import lombok.AllArgsConstructor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
public class RequestHandler implements Runnable {

    private Socket clientSocket;
    @Override
    public void run() {
        try(BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter printWriter = new PrintWriter(clientSocket.getOutputStream(), true)) {
            String clientCommand;
            List<String> elements = new ArrayList<>();
            while ((clientCommand = bufferedReader.readLine()) != null) {
                System.out.println("clientCommand: " + clientCommand);
                if (clientCommand.startsWith("*")) {
                    int arrayLen = Integer.valueOf(clientCommand.substring(1));
                } else if (Character.isLetter(clientCommand.charAt(0))) {
                    elements.add(clientCommand.toLowerCase());
                }

                if (elements.isEmpty()) continue;

                switch (elements.get(0)) {
                    case "ping":
                        printWriter.print("+PONG\r\n");
                        printWriter.flush();
                        break;
                    case "echo":
                        bufferedReader.readLine();
                        elements.add(bufferedReader.readLine());
                        printWriter.print("$" + elements.get(1).length() + "\r\n" + elements.get(1) + "\r\n");
                        printWriter.flush();
                        break;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
