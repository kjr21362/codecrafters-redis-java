import lombok.AllArgsConstructor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.HashMap;

@AllArgsConstructor
public class RequestHandler implements Runnable {

    private Socket clientSocket;
    private String[] args;

    @Override
    public void run() {
        Path dir = Path.of("");
        String dbfilename = "test.rdb";

        if (args.length > 3) {
            if (args[0].equals("--dir")) {
                dir = Path.of(args[1]);
            }
            if (args[2].equals("--dbfilename")) {
                dbfilename = args[3];
            }
        }

        File dbfile = new File(dir.resolve(dbfilename).toString());
        try {
            dbfile.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter printWriter = new PrintWriter(clientSocket.getOutputStream(), true);
             InputStream inputStream = new FileInputStream(dbfile)) {
            String clientCommand;
            HashMap<String, String> store = new HashMap<>();
            HashMap<String, LocalDateTime> expiry = new HashMap<>();
            int arrayLen = 0;

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
                                if (store.containsKey(get_key)) {
                                    String get_value = store.get(get_key);
                                    printWriter.print("$" + get_value.length() + "\r\n" + get_value + "\r\n");
                                    printWriter.flush();
                                    break;
                                }

                                int read;
                                while ((read = inputStream.read()) != -1) {
                                    if (read == 0xFB) {
                                        getLen(inputStream);
                                        getLen(inputStream);
                                        break;
                                    }
                                }

                                int type = inputStream.read();
                                int len = getLen(inputStream);

                                byte[] key_bytes = new byte[len];
                                inputStream.read(key_bytes);
                                String key_str = new String(key_bytes);

                                int value_len = getLen(inputStream);
                                byte[] value_bytes = new byte[value_len];
                                inputStream.read(value_bytes);
                                String value_str = new String(value_bytes);
                                store.put(key_str, value_str);
                                printWriter.print("$" + value_str.length() + "\r\n" + value_str + "\r\n");
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
                        case "keys":
                            bufferedReader.readLine();
                            String db_op = bufferedReader.readLine();
                            switch (db_op) {
                                case "*":
                                    int read;
                                    while ((read = inputStream.read()) != -1) {
                                        if (read == 0xFB) {
                                            getLen(inputStream);
                                            getLen(inputStream);
                                            break;
                                        }
                                    }

                                    int type = inputStream.read();
                                    int len = getLen(inputStream);

                                    byte[] key_bytes = new byte[len];
                                    inputStream.read(key_bytes);
                                    String parsed_key = new String(key_bytes);
                                    printWriter.print("*1\r\n$" + parsed_key.length() + "\r\n" + parsed_key + "\r\n");
                                    printWriter.flush();

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

    private static int getLen(InputStream inputStream) throws IOException {
        int read;
        read = inputStream.read();
        int len_encoding_bit = (read & 0b11000000) >> 6;
        int len = 0;
        //System.out.println("bit: " + (read & 0x11000000));
        if (len_encoding_bit == 0) {
            len = read & 0b00111111;
        } else if (len_encoding_bit == 1) {
            int extra_len = inputStream.read();
            len = ((read & 0b00111111) << 8) + extra_len;
        } else if (len_encoding_bit == 2) {
            byte[] extra_len = new byte[4];
            inputStream.read(extra_len);
            len = ByteBuffer.wrap(extra_len).getInt();
        }
        return len;
    }
}
