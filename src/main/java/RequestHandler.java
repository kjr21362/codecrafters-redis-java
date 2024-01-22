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
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@AllArgsConstructor
public class RequestHandler implements Runnable {

    private Socket clientSocket;
    private String[] args;

    @Override
    public void run() {
        Path dir = Path.of("");
        String dbfilename = "test.rdb";
        Map<String, Entry> store;

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
        try (InputStream inputStream = new FileInputStream(dbfile)) {
            store = readRDBFile(inputStream);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter printWriter = new PrintWriter(clientSocket.getOutputStream(), true)) {
            String clientCommand;
            int arrayLen = 0;

            while ((clientCommand = bufferedReader.readLine()) != null) {
                System.out.println("clientCommand: " + clientCommand);
                if (clientCommand.startsWith("*")) {
                    arrayLen = Integer.parseInt(clientCommand.substring(1));
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
                            long duration_ms = 0;
                            if (arrayLen == 5) {
                                bufferedReader.readLine();
                                bufferedReader.readLine(); // px
                                bufferedReader.readLine();
                                duration_ms = Integer.parseInt(bufferedReader.readLine());
                            }

                            if (duration_ms > 0) {
                                store.put(key, new Entry(key, value, Instant.now().plusMillis(duration_ms)));
                            } else {
                                store.put(key, new Entry(key, value, null));
                            }
                            printWriter.print("$" + "OK".length() + "\r\n" + "OK" + "\r\n");
                            printWriter.flush();
                            break;
                        case "get":
                            bufferedReader.readLine();
                            String get_key = bufferedReader.readLine();
                            Entry cur_entry = store.get(get_key);
                            if (cur_entry == null) {
                                throw new RuntimeException("no entry for key: " + get_key);
                            }

                            if (cur_entry.getExpiry_ms() == null || cur_entry.getExpiry_ms().isAfter(Instant.now())) {
                                printWriter.print("$" + cur_entry.getValue().length() + "\r\n" + cur_entry.getValue() + "\r\n");
                                printWriter.flush();
                                break;
                            } else {
                                store.remove(get_key);
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
                                            printWriter.print("*2\r\n$3\r\ndir\r\n$" + dir.toString().length() + "\r\n" + dir + "\r\n");
                                            printWriter.flush();
                                            break;
                                        case "dbfilename":
                                            printWriter.print("*2\r\n$10\r\ndbfilename\r\n$" + dbfilename.length() + "\r\n" + dbfilename + "\r\n");
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
                                    Set<String> keys = store.keySet();
                                    int n_keys = keys.size();
                                    StringBuilder stringBuilder = new StringBuilder();
                                    for (String key_s : keys) {
                                        stringBuilder.append("$").append(key_s.length()).append("\r\n").append(key_s).append("\r\n");
                                    }

                                    printWriter.print("*" + n_keys + "\r\n" + stringBuilder);
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

    private static Map<String, Entry> readRDBFile(InputStream inputStream) throws IOException {
        int read;
        Map<String, Entry> store = new HashMap<>();

        while ((read = inputStream.read()) != -1) {
            if (read == 0xFB) {
                getLen(inputStream);
                getLen(inputStream);
                break;
            }
        }

        while (true) {
            int indicator = inputStream.read();
            if (indicator == 0xFF || indicator == 0xFE || indicator == -1) break;

            byte[] expiry_time = null;
            int type;
            if (indicator == 0xFD) { // expiry time in seconds
                expiry_time = new byte[4];
                inputStream.read(expiry_time);
                type = inputStream.read();
            } else if (indicator == 0xFC) { // expiry time in ms
                expiry_time = new byte[8];
                inputStream.read(expiry_time);
                type = inputStream.read();
            } else {
                type = indicator;
            }
            int len = getLen(inputStream);

            byte[] key_bytes = new byte[len];
            inputStream.read(key_bytes);
            String parsed_key = new String(key_bytes);
            int value_len = getLen(inputStream);
            byte[] value_bytes = new byte[value_len];
            inputStream.read(value_bytes);
            String value_str = new String(value_bytes);

            Instant expiry_ms = null;
            if (expiry_time != null) {
                if (expiry_time.length == 4) { // seconds
                    expiry_ms = Instant.ofEpochSecond(Integer.toUnsignedLong(ByteBuffer.wrap(expiry_time).order(ByteOrder.LITTLE_ENDIAN).getInt()));
                } else if (expiry_time.length == 8) { // ms
                    expiry_ms = Instant.ofEpochMilli(ByteBuffer.wrap(expiry_time).order(ByteOrder.LITTLE_ENDIAN).getLong());
                }
            }

            store.put(parsed_key, new Entry(parsed_key, value_str, expiry_ms));
        }

        return store;
    }

    private static int getLen(InputStream inputStream) throws IOException {
        int read;
        read = inputStream.read();
        int len_encoding_bit = (read & 0b11000000) >> 6;
        int len = 0;
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
