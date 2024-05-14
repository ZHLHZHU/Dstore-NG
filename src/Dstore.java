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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Dstore {

    /**
     * controller port
     */
    private final int controllerPort;
    /**
     * listen port for client connections
     */
    private final int listenPort;
    private final ServerSocket serverSocket;
    /**
     * file manager
     */
    private final DstoreFileManager fileManager;
    /**
     * timeout
     */
    private final int timeout;
    /**
     * socket to communicate with controller
     */
    private Socket controllerSocket;
    private BufferedReader controllerRx;
    private PrintWriter controllerTx;
    /**
     * running flag
     */
    private volatile boolean running = true;


    public Dstore(int listenPort, int controllerPort, int timeout, String fileFolder) {
        this.listenPort = listenPort;
        this.timeout = timeout;
        this.controllerPort = controllerPort;
        try {
            this.fileManager = new DstoreFileManager(fileFolder);

            // connect to controller
            connect2Controller();

            // listen for client connections
            this.serverSocket = new ServerSocket(listenPort);

        } catch (IOException e) {
            DLog.error("[Dstore] failed to init: %s\n", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        int listenPort = Integer.parseInt(args[0]);
        int controllerPort = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String file_folder = args[3];

        new Dstore(listenPort, controllerPort, timeout, file_folder).start();
    }

    private void connect2Controller() throws IOException {
        this.controllerSocket = new Socket("localhost", controllerPort);
        this.controllerRx = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
        this.controllerTx = new PrintWriter(controllerSocket.getOutputStream(), true);
    }

    public void start() {
        running = true;
        DLog.info("[Dstore] started, begin registering with controller\n");

        // start controller handler
        final Thread controllerThread = new Thread(new ControllerHandler());
        controllerThread.start();

        // listen for client connections
        while (running) {
            try {
                final Socket clientSocket = serverSocket.accept();
                clientSocket.setSoTimeout(timeout);
                DLog.info("[Dstore] client: %s connected\n", clientSocket.getPort());
                new Thread(new ClientHandler(clientSocket)).start();
            } catch (IOException e) {
                DLog.error("[Dstore] failed to accept client connection: %s\n", e.getMessage());
            }
        }
    }

    public void send(String... msg) {
        final String body = String.join(" ", msg);
        DLog.debug("[Dstore] sending message: %s\n", body);
        controllerTx.println(body);
    }

    /**
     * Communicate with controller
     */
    class ControllerHandler implements Runnable {

        private final ExecutorService rebalanceTransferExecutorService = Executors.newFixedThreadPool(4);

        @Override
        public void run() {
            // Join
            send(Protocol.JOIN_TOKEN, String.valueOf(listenPort));
            int retry = 1;
            while (running) {
                try {
                    final String line = controllerRx.readLine();
                    if (line == null) {
                        DLog.error("[Dstore] connection with controller closed\n");
                        break;
                    }
                    DLog.debug("[Controller]->[Dstore] received message: %s\n", line);
                    retry = 1;
                    final String[] tokens = line.split(" ");
                    final String command = tokens[0];

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
                            DLog.error("[Dstore] unknown command: %s\n", command);
                            break;
                    }

                } catch (IOException e) {
                    DLog.error("[Dstore] failed to read message from controller: %s\n", e.getMessage());
                    if (!controllerSocket.isConnected()) {
                        // retry to connect to controller
                        while (running) {
                            try {
                                DLog.info("[Dstore] retrying to connect to controller in %d seconds\n", retry);
                                Thread.sleep(retry * 1000L);
                                connect2Controller();
                                send(Protocol.JOIN_TOKEN, String.valueOf(listenPort));
                                break;
                            } catch (IOException | InterruptedException ex) {
                                DLog.error("[Dstore] failed to connect to controller: %s\n", ex.getMessage());
                                retry = Math.min(retry * 2, 32);
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
                int destNum = Integer.parseInt(tokens[startIndex++]);
                List<String> dests = new ArrayList<>();
                for (int j = 0; j < destNum; j++) {
                    dests.add(tokens[startIndex++]);
                }
                filesToSend.put(file, dests);
            }

            int removeNum = Integer.parseInt(tokens[startIndex++]);
            List<String> filesToRemove = new ArrayList<>();
            for (int i = 0; i < removeNum; i++) {
                filesToRemove.add(tokens[startIndex++]);
            }

            // transfer files
            List<Future<?>> transferFutures = new ArrayList<>();
            for (Map.Entry<String, List<String>> entry : filesToSend.entrySet()) {
                String file = entry.getKey();
                final Optional<String> load = fileManager.load(file);
                final Optional<Long> _fileSize = fileManager.fileSize(file);
                if (load.isEmpty() || _fileSize.isEmpty()) {
                    DLog.error("[Dstore] failed to load file: %s\n", file);
                    continue;
                }
                String content = load.get();
                for (String dest : entry.getValue()) {
                    final Future<?> future = rebalanceTransferExecutorService.submit(() -> {
                        try (var socket = new Socket("localhost", Integer.parseInt(dest))) {
                            socket.setSoTimeout(timeout);
                            var rx = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            var tx = new PrintWriter(socket.getOutputStream(), true);
                            tx.println(Protocol.REBALANCE_STORE_TOKEN + " " + file + " " + _fileSize.get());
                            final String res = rx.readLine();
                            if (res == null || !res.equals(Protocol.ACK_TOKEN)) {
                                DLog.error("[Dstore] failed to receive ack from %s\n", dest);
                                return;
                            }
                            try (var reader = new BufferedReader(new StringReader(content))) {
                                String line;
                                while ((line = reader.readLine()) != null) {
                                    tx.println(line);
                                }
                            }
                            DLog.debug("[Dstore] transfer file: %s to %s\n", file, dest);
                        } catch (Exception e) {
                            DLog.error("[Dstore] failed to connect to %s: %s\n", dest, e.getMessage());
                        }
                    });
                    transferFutures.add(future);
                }
            }
            transferFutures.forEach(f -> {
                try {
                    f.get();
                } catch (Exception e) {
                    DLog.error("[Dstore] failed to transfer file: %s\n", e.getMessage());
                }
            });

            // Remove files
            for (String file : filesToRemove) {
                fileManager.getFileMap().remove(file);
                fileManager.delete(file);
                DLog.debug("[Dstore] remove file: %s\n", file);
            }

            // Send rebalance complete to controller
            controllerTx.println(Protocol.REBALANCE_COMPLETE_TOKEN);
            DLog.debug("[Dstore] send rebalance complete\n");
        }

        private void onControllerList() {
            final ArrayList<String> files = new ArrayList<>(fileManager.getFileMap().keySet());
            final String body = String.join(" ", files);
            controllerTx.println(Protocol.LIST_TOKEN + " " + body);
            DLog.debug("send list ack: %s\n", Protocol.LIST_TOKEN + " " + body);
        }

        private void onControllerRemove(String[] tokens) {
            String filename = tokens[1];
            fileManager.getFileMap().remove(filename);
            fileManager.delete(filename);
            DLog.debug("[Dstore] remove file: %s\n", filename);
            controllerTx.println(Protocol.REMOVE_ACK_TOKEN + " " + filename);
            DLog.debug("send remove ack: %s\n", Protocol.REMOVE_ACK_TOKEN + " " + filename);
        }
    }

    /**
     * Communicate with client
     */
    class ClientHandler implements Runnable {

        private final Socket clientSocket;

        public ClientHandler(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {
            try {
                InputStream rawRx = clientSocket.getInputStream();
                OutputStream rawTx = clientSocket.getOutputStream();
                final BufferedReader rx = new BufferedReader(new InputStreamReader(rawRx));
                final PrintWriter tx = new PrintWriter(rawTx, true);
                while (true) {
                    final String line = rx.readLine();
                    if (line == null) {
//                        DLog.info("receive empty message, client: %s disconnected\n", clientSocket.getPort());
                        break;
                    }
                    DLog.debug("[Dstore] received message: %s\n", line);
                    final String[] tokens = line.split(" ");
                    final String command = tokens[0];
                    switch (command) {
                        case Protocol.STORE_TOKEN:
                            onClientStore(tx, rx, rawRx, tokens);
                            break;

                        case Protocol.LOAD_DATA_TOKEN:
                            onClientLoadData(tx, rawTx, tokens);
                            break;

                        case Protocol.REBALANCE_STORE_TOKEN:
                            DLog.debug("[Dstore]->[Dstore] rebalance store\n");
                            onRebalanceStore(rx, tx, rawRx, tokens);
                            break;
                        default:
                            DLog.error("[Dstore] unknown command: %s\n", command);
                    }
                }
            } catch (IOException e) {
                DLog.error("[Dstore] failed to read message from client: %s\n", e.getMessage());
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    DLog.error("[Dstore] failed to close client socket: %s\n", e.getMessage());
                }
            }
        }

        private void onClientLoadData(PrintWriter tx, OutputStream rawTx, String[] tokens) {
            final String fileName = tokens[1];
            final var _fileContent = fileManager.loadRaw(fileName);
            if (_fileContent.isEmpty()) {
                tx.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                return;
            }
            final Byte[] fileContent = _fileContent.get();
            DLog.debug("[Dstore] send file: %s to client\n", fileName);
            try {
                rawTx.write(toPrimitive(fileContent));
                rawTx.flush();
            } catch (IOException e) {
                DLog.error("[Dstore] failed to send file: %s\n", e.getMessage());
            }
        }

        private void onClientStore(PrintWriter tx, BufferedReader rx, InputStream rawRx, String[] tokens) throws IOException {
            final String filename = tokens[1];
            final long fileSize = Long.parseLong(tokens[2]);

            fileManager.getFileMap().put(filename, fileSize);
            tx.println(Protocol.ACK_TOKEN);

            // receive data until file size
            ByteArrayOutputStream bufferStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int bytesRead;
            long totalBytesRead = 0;
            while (totalBytesRead < fileSize) {
                bytesRead = rawRx.read(buffer, 0, Math.min(buffer.length, (int) (fileSize - totalBytesRead)));
                if (bytesRead == -1) {
                    break;
                }
                bufferStream.write(buffer, 0, bytesRead);
                totalBytesRead += bytesRead;
            }

            if (totalBytesRead == fileSize) {
                fileManager.store(filename, bufferStream.toByteArray());
                DLog.info("successfully stored file: %s, ack to controller", filename);
                controllerTx.println(Protocol.STORE_ACK_TOKEN + " " + filename);
            } else {
                DLog.info("failed to receive complete file: %s, received %d of %d bytes", filename, totalBytesRead, fileSize);
            }
        }

        private void onRebalanceStore(BufferedReader rx, PrintWriter tx, InputStream rawRx, String[] tokens) throws IOException {
            onClientStore(tx, rx, rawRx, tokens);
        }

        public byte[] toPrimitive(Byte[] byteObjects) {
            byte[] bytes = new byte[byteObjects.length];
            for (int i = 0; i < byteObjects.length; i++) {
                bytes[i] = byteObjects[i];
            }
            return bytes;
        }
    }
}

class DstoreFileManager {

    private final String baseDir;

    private final ConcurrentHashMap<String, Long> fileMap = new ConcurrentHashMap<>();

    /**
     * cleanup the base dir on startup
     */
    public DstoreFileManager(String baseDir) {
        this.baseDir = baseDir;
        final File dir = new File(baseDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        final File[] files = dir.listFiles();
        Optional.ofNullable(files).stream().flatMap(Arrays::stream).filter(File::isFile).forEach(File::delete);
    }

    public ConcurrentHashMap<String, Long> getFileMap() {
        return fileMap;
    }

    /**
     * store file
     */
    public boolean store(String filename, byte[] fileContent) {
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


    public boolean delete(String filename) {
        final File file = new File(baseDir + "/" + filename);
        if (file.exists()) {
            return file.delete();
        }
        return false;
    }

    public Optional<String> load(String filename) {
        final File file = new File(baseDir + "/" + filename);
        if (!file.exists()) {
            return Optional.empty();
        }
        try (final FileReader fileReader = new FileReader(file)) {
            final char[] buffer = new char[1024];
            final StringBuilder stringBuilder = new StringBuilder();
            int read;
            while ((read = fileReader.read(buffer)) != -1) {
                stringBuilder.append(buffer, 0, read);
            }
            return Optional.of(stringBuilder.toString());
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    public Optional<Byte[]> loadRaw(String filename) {
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

class DLog {

    private static final DateTimeFormatter dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");


    public static void debug(String msg, Object... args) {
        log("D", msg, args);
    }

    public static void info(String msg, Object... args) {
        log("I", msg, args);
    }

    public static void warn(String msg, Object... args) {
        log("W", msg, args);
    }

    public static void error(String msg, Object... args) {
        log("E", msg, args);
    }

    private static void log(String level, String msg, Object... args) {
        if (msg.endsWith("\n")) {
            msg = msg.substring(0, msg.length() - 1);
        }
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        final String currentCall = stackTrace.length > 3 ? stackTrace[3].toString() : "-";
        System.out.printf("%s [%s] %s %s\n", LocalDateTime.now().format(dateTimeFormat), level, currentCall, String.format(msg, args));
    }
}
