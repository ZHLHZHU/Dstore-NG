import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

public class Dstore {

    public static int controllerPort;
    public static int listenPort;
    public static int timeout;
    public static DstoreFileManager fileManager;

    public Dstore(int listenPort, int controllerPort, int timeout, String fileFolder) {
        Dstore.listenPort = listenPort;
        Dstore.timeout = timeout;
        Dstore.controllerPort = controllerPort;
        Dstore.fileManager = new DstoreFileManager(fileFolder);
    }

    public static void main(String[] args) throws IOException {
        int listenPort = Integer.parseInt(args[0]);
        int controllerPort = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String file_folder = args[3];

        new Dstore(listenPort, controllerPort, timeout, file_folder).start();
    }


    public void start() throws IOException {
        Log1.INFO.log("[Dstore] starting server on port: %d", listenPort);

        // start controller handler
        final Thread controllerThread = new Thread(new ControllerHandler());
        controllerThread.start();

        ServerSocket serverSocket = new ServerSocket(listenPort);

        // listen for client connections
        while (true) {
            try {
                final Socket clientSocket = serverSocket.accept();
                clientSocket.setSoTimeout(timeout);
                new Thread(new RecClient(clientSocket)).start();
                Log1.INFO.log("[Dstore] accepted client connection");
            } catch (IOException e) {
                Log1.ERROR.log("[Dstore] failed to accept client connection: %s", e.getMessage());
            }
        }
    }


}

class ControllerHandler implements Runnable {

    public static Socket socket;
    public static BufferedReader controllerRec;
    public static PrintWriter controllerSender;


    private void reconnect() throws IOException {
        ControllerHandler.socket = new Socket("localhost", Dstore.controllerPort);
        ControllerHandler.controllerRec = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        ControllerHandler.controllerSender = new PrintWriter(socket.getOutputStream(), true);
    }

    @Override
    public void run() {
        try {
            reconnect();
        } catch (IOException e) {
            Log1.ERROR.log("[Controller] failed to connect to controller: %s", e.getMessage());
            return;
        }
        // Join
        final String body = Protocol.JOIN_TOKEN + " " + Dstore.listenPort;
        ControllerHandler.controllerSender.println(body);

        while (true) {
            try {
                final String line = controllerRec.readLine();
                if (line == null) {
                    Log1.WARN.log("[Controller] connection closed by controller");
                    break;
                }
                final String[] tokens = line.split(" ");
                final String command = tokens[0];
                Log1.INFO.log("[Controller] received: %s", line);
                switch (command) {
                    case Protocol.REMOVE_TOKEN:
                        onControllerRemove(tokens);
                        break;
                    case Protocol.LIST_TOKEN:
                        onControllerList();
                        break;
                    case Protocol.REBALANCE_TOKEN:
                        // Parse the list of files to send and remove
                        onRebalance(tokens);
                        break;
                    default:
                        Log1.ERROR.log("[Dstore] unknown command: %s", command);
                        break;
                }

            } catch (IOException e) {
                Log1.ERROR.log("[Controller] failed to read message from controller: %s", e.getMessage());
                if (!socket.isConnected()) {
                    // retry to connect to controller
                    while (true) {
                        try {
                            Thread.sleep(Dstore.timeout * 2L);
                            reconnect();
                            ControllerHandler.controllerSender.println(Protocol.JOIN_TOKEN + " " + Dstore.listenPort);
                            break;
                        } catch (IOException | InterruptedException ex) {
                            Log1.ERROR.log("[Controller] failed to reconnect: %s", ex.getMessage());
                            continue;
                        }
                    }
                }
            }
        }
    }

    private void onRebalance(String[] tokens) {
        int sendNum = Integer.parseInt(tokens[1]);
        int startIndex = 2;
        Map<String, List<String>> filesToSend = new HashMap<>();
        for (int i = 0; i < sendNum; i++) {
            String file = tokens[startIndex++];
            final int destNum = Integer.parseInt(tokens[startIndex++]);
            Arrays.stream(tokens).skip(startIndex)
                    .limit(destNum)
                    .forEach(to -> filesToSend.computeIfAbsent(file, k -> new ArrayList<>()).add(to));
        }

        int removeNum = Integer.parseInt(tokens[startIndex++]);
        List<String> filesToRemove = new ArrayList<>();
        for (int i = 0; i < removeNum; i++) {
            filesToRemove.add(tokens[startIndex++]);
        }

        final List<Thread> threads = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : filesToSend.entrySet()) {
            String file = entry.getKey();
            final Optional<String> load = Dstore.fileManager.fetch(file);
            final Optional<Long> _fileSize = Dstore.fileManager.fileSize(file);
            if (load.isEmpty() || _fileSize.isEmpty()) {
                continue;
            }
            String content = load.get();
            for (String dest : entry.getValue()) {
                final Thread thread = new Thread(() -> {
                    try (var socket = new Socket("localhost", Integer.parseInt(dest))) {
                        PrintWriter sender = new PrintWriter(socket.getOutputStream(), true);
                        sender.println(Protocol.REBALANCE_STORE_TOKEN + " " + file + " " + _fileSize.get());
                        BufferedReader rec = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        final String res = rec.readLine();
                        if (res == null || !res.equals(Protocol.ACK_TOKEN)) {
                            return;
                        }
                        try (var reader = new BufferedReader(new StringReader(content))) {
                            String line;
                            while ((line = reader.readLine()) != null) {
                                sender.println(line);
                            }
                        }
                    } catch (Exception e) {
                        Log1.ERROR.log("[Dstore] failed to transfer file: %s", e.getMessage());
                    }
                });
                threads.add(thread);
                thread.start();
            }
        }
        // join with timeout
        for (Thread thread : threads) {
            try {
                thread.join(Dstore.timeout);
            } catch (InterruptedException e) {
                Log1.ERROR.log("[Dstore] failed to join thread: %s", e.getMessage());
            }
        }

        // Remove files
        for (String file : filesToRemove) {
            Dstore.fileManager.getFileMap().remove(file);
            Dstore.fileManager.remove(file);
        }

        // Send rebalance complete to controller
        controllerSender.println(Protocol.REBALANCE_COMPLETE_TOKEN);
    }

    private void onControllerList() {
        final ArrayList<String> files = new ArrayList<>(Dstore.fileManager.getFileMap().keySet());
        final String body = String.join(" ", files);
        controllerSender.println(Protocol.LIST_TOKEN + " " + body);
    }

    private void onControllerRemove(String[] tokens) {
        String filename = tokens[1];
        Dstore.fileManager.getFileMap().remove(filename);
        Dstore.fileManager.remove(filename);
        controllerSender.println(Protocol.REMOVE_ACK_TOKEN + " " + filename);
    }
}


class RecClient implements Runnable {

    private final Socket clientSocket;

    public RecClient(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try {
            InputStream inputStream = clientSocket.getInputStream();
            OutputStream outputStream = clientSocket.getOutputStream();
            final BufferedReader rx = new BufferedReader(new InputStreamReader(inputStream));
            final PrintWriter tx = new PrintWriter(outputStream, true);
            while (true) {
                final String line = rx.readLine();
                if (line == null) {
                    break;
                }
                final String[] tokens = line.split(" ");
                final String command = tokens[0];
                switch (command) {
                    case Protocol.STORE_TOKEN:
                    case Protocol.REBALANCE_STORE_TOKEN:
                        onStore(tx, inputStream, tokens);
                        break;
                    case Protocol.LOAD_DATA_TOKEN:
                        onLoadData(tx, outputStream, tokens[1]);
                        break;
                    default:
                        Log1.ERROR.log("[Dstore] unknown command: %s", command);
                        break;
                }
            }
        } catch (IOException e) {
            Log1.ERROR.log("[Dstore] failed to read message from client: %s", e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                Log1.ERROR.log("[Dstore] failed to close client socket: %s", e.getMessage());
            }
        }
    }

    private void onLoadData(PrintWriter sender, OutputStream outputStream, String fileName) {
        final var _fileContent = Dstore.fileManager.fetchBytes(fileName);
        if (_fileContent.isEmpty()) {
            sender.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            return;
        }
        final Byte[] fileContent = _fileContent.get();
        try {
            outputStream.write(toPrimitive(fileContent));
            outputStream.flush();
        } catch (IOException e) {
            Log1.ERROR.log("[Dstore] failed to send file data: %s", e.getMessage());
        }
    }

    private void onStore(PrintWriter tx, InputStream rawRx, String[] tokens) throws IOException {
        final String filename = tokens[1];
        final long fileSize = Long.parseLong(tokens[2]);

        Dstore.fileManager.getFileMap().put(filename, fileSize);
        tx.println(Protocol.ACK_TOKEN);

        // receive data until file size
        ByteArrayOutputStream bufferStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[512];
        long totalBytesRead = 0;
        while (totalBytesRead < fileSize) {
            int res = rawRx.read(buffer, 0, Math.min(buffer.length, (int) (fileSize - totalBytesRead)));
            if (res == -1) {
                break;
            }
            bufferStream.write(buffer, 0, res);
            totalBytesRead += res;
        }

        if (totalBytesRead != fileSize) {
            Log1.ERROR.log("[Dstore] failed to receive file data: %s", filename);
            return;
        }
        Dstore.fileManager.save(filename, bufferStream.toByteArray());
        ControllerHandler.controllerSender.println(Protocol.STORE_ACK_TOKEN + " " + filename);
    }

    public byte[] toPrimitive(Byte[] byteObjects) {
        byte[] bytes = new byte[byteObjects.length];
        for (int i = 0; i < byteObjects.length; i++) {
            bytes[i] = byteObjects[i];
        }
        return bytes;
    }
}

class DstoreFileManager {

    private final String baseDir;

    private final Map<String, Long> fileMap = new ConcurrentHashMap<>();

    /**
     * cleanup the base dir on startup
     */
    public DstoreFileManager(String baseDir) {
        this.baseDir = baseDir;
        final File dir = new File(baseDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        Optional.ofNullable(dir.listFiles())
                .stream()
                .flatMap(Arrays::stream)
                .forEach(File::delete);
    }

    public Map<String, Long> getFileMap() {
        return fileMap;
    }


    public boolean save(String filename, byte[] fileContent) {
        // create file with content
        final File file = new File(baseDir + "/" + filename);
        if (file.exists()) {
            return false;
        }
        // write content to file
        try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            fileOutputStream.write(fileContent);
            fileOutputStream.flush();
        } catch (IOException ignored) {
            return false;
        }
        return true;
    }


    public boolean remove(String filename) {
        final File file = new File(baseDir + "/" + filename);
        if (file.exists()) {
            return file.delete();
        }
        return false;
    }

    public Optional<String> fetch(String filename) {
        final File file = new File(baseDir + "/" + filename);
        if (!file.exists()) {
            return Optional.empty();
        }
        try (final FileReader fileReader = new FileReader(file)) {
            final StringBuilder stringBuilder = new StringBuilder();
            int read;
            final char[] buffer = new char[512];
            while ((read = fileReader.read(buffer)) != -1) {
                stringBuilder.append(buffer, 0, read);
            }
            return Optional.of(stringBuilder.toString());
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    public Optional<Byte[]> fetchBytes(String filename) {
        final File file = new File(baseDir + "/" + filename);
        if (!file.exists()) {
            return Optional.empty();
        }
        try (final FileInputStream fileInputStream = new FileInputStream(file)) {
            final byte[] buffer = new byte[1024];
            final List<Byte> byteList = new ArrayList<>();
            int read;
            while ((read = fileInputStream.read(buffer)) != -1) {
                for (int i = 0; i < read; i++) {
                    byteList.add(buffer[i]);
                }
            }
            return Optional.of(byteList.toArray(new Byte[0]));
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    public Optional<Long> fileSize(String filename) {
        final File file = new File(baseDir + "/" + filename);
        if (file.exists()) {
            return Optional.of(file.length());
        }
        return Optional.empty();
    }
}

enum Log1 {
    TRACE, DEBUG, INFO, WARN, ERROR;

    private static final DateTimeFormatter dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public void log(String msg, Object... args) {
        if (this.ordinal() < AnyDoor.logLevel.ordinal()) {
            return;
        }

        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        final String currentCall = stackTrace.length > 2 ? stackTrace[2].toString() : "-";
        System.out.printf("%s [%s] %s %s\n", LocalDateTime.now().format(dateTimeFormat), this.name(), currentCall, String.format(msg, args));
    }
}