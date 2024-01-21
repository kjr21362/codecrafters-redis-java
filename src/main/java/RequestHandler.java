import lombok.AllArgsConstructor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@AllArgsConstructor
public class RequestHandler implements Runnable {

    private Socket clientSocket;
    @Override
    public void run() {
        try(BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter printWriter = new PrintWriter(clientSocket.getOutputStream(), true)) {
            String clientCommand;
            HashMap<String, String> store = new HashMap<>();

            while ((clientCommand = bufferedReader.readLine()) != null) {
                System.out.println("clientCommand: " + clientCommand);
                if (clientCommand.startsWith("*")) {
                    int arrayLen = Integer.valueOf(clientCommand.substring(1));
                } else if (Character.isLetter(clientCommand.charAt(0))) {
                    switch (clientCommand) {
                        case "ping":
                            printWriter.print("+PONG\r\n");
                            printWriter.flush();
                            break;
                        case "echo":
                            bufferedReader.readLine();
                            String message = bufferedReader.readLine();
                            printWriter.print("$" + message.length() + "\r\n" + message + "\r\n");
                            printWriter.flush();
                            break;
                        case "set":
                            bufferedReader.readLine();
                            String key = bufferedReader.readLine();
                            bufferedReader.readLine();
                            String value = bufferedReader.readLine();
                            store.put(key, value);
                            printWriter.print("$" + "OK".length() + "\r\n" + "OK" + "\r\n");
                            printWriter.flush();
                            break;
                        case "get":
                            bufferedReader.readLine();
                            String get_key = bufferedReader.readLine();
                            String get_value = store.get(get_key);
                            printWriter.print("$" + get_value.length() + "\r\n" + get_value + "\r\n");
                            printWriter.flush();
                            break;
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
