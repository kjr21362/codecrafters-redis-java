import lombok.AllArgsConstructor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.HashMap;

@AllArgsConstructor
public class RequestHandler implements Runnable {

    private Socket clientSocket;
    private String[] args;

    @Override
    public void run() {
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter printWriter = new PrintWriter(clientSocket.getOutputStream(), true)) {
            String clientCommand;
            HashMap<String, String> store = new HashMap<>();
            HashMap<String, LocalDateTime> expiry = new HashMap<>();
            int arrayLen = 0;
            Path dir = null;
            String dbfilename = null;

            if (args.length > 3) {
                if (args[0].equals("--dir")) {
                    dir = Path.of(args[1]);
                }
                if (args[2].equals("--dbfilename")) {
                    dbfilename = args[3];
                }
            }

            while ((clientCommand = bufferedReader.readLine()) != null) {
                System.out.println("clientCommand: " + clientCommand);
                if (clientCommand.startsWith("*")) {
                    arrayLen = Integer.valueOf(clientCommand.substring(1));
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
                            int duration_ms = 0;
                            if (arrayLen == 5) {
                                bufferedReader.readLine();
                                bufferedReader.readLine(); // px
                                bufferedReader.readLine();
                                duration_ms = Integer.valueOf(bufferedReader.readLine());
                            }
                            store.put(key, value);
                            if (duration_ms > 0) {
                                expiry.put(key, LocalDateTime.now().plusNanos(duration_ms * 1000000));
                            }
                            printWriter.print("$" + "OK".length() + "\r\n" + "OK" + "\r\n");
                            printWriter.flush();
                            break;
                        case "get":
                            bufferedReader.readLine();
                            String get_key = bufferedReader.readLine();
                            if (!expiry.containsKey(get_key) || expiry.get(get_key).isAfter(LocalDateTime.now())) {
                                String get_value = store.get(get_key);
                                printWriter.print("$" + get_value.length() + "\r\n" + get_value + "\r\n");
                                printWriter.flush();
                            } else {
                                store.remove(get_key);
                                expiry.remove(get_key);
                                printWriter.print("$-1\r\n");
                                printWriter.flush();

                            }
                            break;
                        case "config":
                            bufferedReader.readLine();
                            String op = bufferedReader.readLine();
                            switch (op) {
                                case "get":
                                    bufferedReader.readLine();
                                    String param = bufferedReader.readLine();
                                    switch (param) {
                                        case "dir":
                                            printWriter.print("*2\r\n$3\r\ndir\r\n$" + dir.toString().length() + "\r\n" + dir.toString() + "\r\n");
                                            printWriter.flush();
                                            break;
                                        case "dbfilename":
                                            printWriter.print("*2\r\n$10\r\ndbfilename\r\n$" + dbfilename.toString().length() + "\r\n" + dbfilename.toString() + "\r\n");
                                            printWriter.flush();
                                            break;
                                    }
                                    break;
                            }
                            break;
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
