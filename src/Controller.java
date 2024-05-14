import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

enum Log {
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR;

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

interface Handler extends Runnable {

    default void res(String message) throws InterruptedException {
        getOutQueue().put(new AbstractMap.SimpleImmutableEntry<>(Peer.OutMessageType.RES, message));
    }

    default void broadcast(String message) throws InterruptedException {
        getOutQueue().put(new AbstractMap.SimpleImmutableEntry<>(Peer.OutMessageType.BROADCAST, message));
    }

    BlockingQueue<Map.Entry<Peer.InMessageType, String>> getInQueue();

    BlockingQueue<Map.Entry<Peer.OutMessageType, String>> getOutQueue();

}

public class Controller {

    public static void main(String[] args) throws IOException {
        AnyDoor.port = Integer.parseInt(args[0]);
        AnyDoor.replicate = Integer.parseInt(args[1]);
        AnyDoor.timeout = Long.parseLong(args[2]);
        AnyDoor.rebalanced_period = Integer.parseInt(args[3]);

        // cleanup expired files
        AnyDoor.fileManager.timer(AnyDoor.timeout * 2);

        ServerSocket serverSocket = new ServerSocket(AnyDoor.port);
        Log.INFO.log("Controller started at port %d", AnyDoor.port);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            Log.INFO.log("Accepted connection from %s:%d", clientSocket.getInetAddress().getHostAddress(), clientSocket.getPort());
            new Thread(() -> {
                try {
                    Peer peer = new Peer(clientSocket);
                    peer.run();
                } catch (IOException e) {
                    Log.ERROR.log("Error while creating peer");
                }
            }).start();
        }
    }

}

class Peer {

    protected final BufferedReader in;
    protected final PrintWriter out;
    protected final InputStream inputStream;
    protected final OutputStream outputStream;
    private final Socket socket;
    /**
     * 0 dstore; 1 client
     */
    private Integer type;

    private Handler handler;

    public Peer(Socket socket) throws IOException {
        this.socket = socket;
        this.inputStream = socket.getInputStream();
        this.outputStream = socket.getOutputStream();
        this.in = new BufferedReader(new InputStreamReader(inputStream));
        this.out = new PrintWriter(new OutputStreamWriter(outputStream), true);
    }

    public void run() {
        BlockingQueue<Map.Entry<Peer.InMessageType, String>> inQueue = new LinkedBlockingQueue<>();
        BlockingQueue<Map.Entry<Peer.OutMessageType, String>> outQueue = new LinkedBlockingQueue<>();

        Thread receiveThread = new Thread(() -> receiveMessage(inQueue, outQueue));
        receiveThread.start();

        Thread sendThread = new Thread(() -> sendMessage(outQueue));
        sendThread.start();
    }

    public void sendMessage(BlockingQueue<Map.Entry<Peer.OutMessageType, String>> outQueue) {
        while (true) {
            try {
                Map.Entry<Peer.OutMessageType, String> res = outQueue.take();
                switch (res.getKey()) {
                    case RES:
                        Log.TRACE.log(String.format("✉\uFE0F->[%s] %s", type == 0 ? "Dstore" : "Client", res.getValue()));
                        out.println(res.getValue());
                        break;
                    case BROADCAST:
                        AnyDoor.onlineClients.forEach(client -> {
                            try {
                                client.getInQueue().put(new AbstractMap.SimpleImmutableEntry<>(
                                        Peer.InMessageType.EVENT,
                                        res.getValue()));
                            } catch (InterruptedException e) {
                                Log.ERROR.log("Error while broadcasting message");
                            }
                        });
                        break;
                }
                Log.TRACE.log("Sent message: %s", res);
            } catch (InterruptedException e) {
                Log.ERROR.log("Error while taking message from outQueue");
                break;
            }
        }
    }

    public void receiveMessage(BlockingQueue<Map.Entry<Peer.InMessageType, String>> inQueue, BlockingQueue<Map.Entry<Peer.OutMessageType, String>> outQueue) {
        while (true) {
            try {
                String message = in.readLine();
                if (message == null) {
                    Log.INFO.log("Connection closed");
                    break;
                }
                if (type == null) {
                    if (message.startsWith(Protocol.JOIN_TOKEN)) {
                        handler = onJoin(message, inQueue, outQueue);
                        continue;
                    } else {
                        type = 1;
                        handler = new ClientHandler(inQueue, outQueue);
                        AnyDoor.onlineClients.add((ClientHandler) handler);
                    }
                }
//                 dispatch message
                new Thread(handler).start();
                // forward message to handler
                handler.getInQueue().put(new AbstractMap.SimpleImmutableEntry<>(InMessageType.REQ, message));
            } catch (IOException | InterruptedException e) {
                Log.ERROR.log("Error while reading message from socket");
                break;
            }
        }
    }

    private DstoreHandler onJoin(String message, BlockingQueue<Map.Entry<InMessageType, String>> inQueue, BlockingQueue<Map.Entry<OutMessageType, String>> outQueue) {
        Log.INFO.log("Dstore joined: %s", message);
        type = 0;
        int dstorePort = Integer.parseInt(message.split(" ")[1]);
        DstoreHandler newDstore = new DstoreHandler(inQueue, outQueue, dstorePort);
        AnyDoor.onlineDstores.put(dstorePort, newDstore);
        // TODO rebalance on join
        return newDstore;
    }

    public enum InMessageType {
        // request from peer
        REQ,

        // Global Event
        EVENT,
        ;
    }

    public enum OutMessageType {
        // response to peer
        RES,

        // Global Event
        BROADCAST,
        ;

    }
}

class AnyDoor {

    public static final Map<Integer, DstoreHandler> onlineDstores = new ConcurrentHashMap<>();
    public static final List<ClientHandler> onlineClients = new CopyOnWriteArrayList<>();
    public static final FileManager fileManager = new FileManager();
    public static int port;
    public static int replicate;
    public static int rebalanced_period;
    public static Log logLevel = Log.TRACE;
    public static long timeout = 1000;

}

class FileManager {

    private final Map<String, FileHandler> fileMap = new HashMap<>();


    public boolean contains(String filename) {
        return fileMap.containsKey(filename);
    }

    public Optional<FileHandler> startStoring(String filename, long fileSize) {
        FileHandler old = fileMap.putIfAbsent(filename, new FileHandler(filename, fileSize));
        if (old != null) {
            return Optional.empty();
        }
        return Optional.of(fileMap.get(filename));
    }

    public Optional<FileHandler> fetch(String filename) {
        FileHandler file = fileMap.get(filename);
        if (file == null) {
            return Optional.empty();
        }
        return Optional.of(file);
    }

    public List<String> list() {
        return fileMap.values().stream()
                .filter(FileHandler::visible)
                .map(FileHandler::getFileName)
                .toList();
    }

    public List<FileHandler> listFiles() {
        return fileMap.values().stream()
                .filter(FileHandler::visible)
                .toList();
    }

    public void updateFromDstore(List<String> fileLists, int dstorePort) {
        long now = System.currentTimeMillis();

        DstoreHandler dstorePeer = AnyDoor.onlineDstores.get(dstorePort);
        Set<String> onDstoreFiles = fileMap.values().stream().filter(f -> f.getOnDstores().contains(dstorePeer)).map(FileHandler::getFileName).collect(Collectors.toSet());

        Set<String> willRemove = new HashSet<>(onDstoreFiles);
        fileLists.forEach(willRemove::remove);

        Set<String> willAdd = new HashSet<>(fileLists);
        willAdd.removeAll(onDstoreFiles);

        willRemove.stream().map(fileMap::get).forEach(f -> f.removeDstore(dstorePeer));
        willAdd.forEach(f -> fileMap.get(f).getOnDstores().add(dstorePeer));
    }

    public void timer(long interval) {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            fileMap.values().stream()
                    .filter(f -> f.expired(now) && !f.visible())
                    .forEach(f -> {
                        if (f.getLock().tryLock()) {
                            try {
                                fileMap.remove(f.getFileName());
                            } finally {
                                f.getLock().unlock();
                            }
                        }
                    });
        }, 0, interval, java.util.concurrent.TimeUnit.MILLISECONDS);
    }
}

class DstoreHandler implements Handler {

    private final Integer port;

    private final BlockingQueue<Map.Entry<Peer.InMessageType, String>> inQueue;
    private final BlockingQueue<Map.Entry<Peer.OutMessageType, String>> outQueue;

    public DstoreHandler(BlockingQueue<Map.Entry<Peer.InMessageType, String>> inQueue,
                         BlockingQueue<Map.Entry<Peer.OutMessageType, String>> outQueue,
                         int port) {
        this.inQueue = inQueue;
        this.outQueue = outQueue;
        this.port = port;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Map.Entry<Peer.InMessageType, String> res = inQueue.take();
                switch (res.getKey()) {
                    case REQ:
                        dispatchReq(res.getValue());
                        break;
                    case EVENT:
                        dispatchEvent(res.getValue());
                        break;
                }
                Log.INFO.log("Received message: %s", res);
            } catch (InterruptedException e) {
                Log.ERROR.log("Error while taking message from inQueue");
                break;
            }
        }
    }

    public void dispatchReq(String message) {
        final String[] tokens = message.split(" ");
        final String opt = tokens[0];
        switch (opt) {
            case Protocol.STORE_ACK_TOKEN:
                onStoreAck(tokens[1]);
                break;
            case Protocol.REMOVE_ACK_TOKEN:
                onRemoveAck(tokens[1]);
                break;
            case Protocol.LIST_TOKEN:
                final List<String> fileNames = tokens.length > 1 ? Arrays.stream(tokens).skip(1).toList()
                        : Collections.emptyList();
                onList(fileNames);
                break;
            case Protocol.REBALANCE_COMPLETE_TOKEN:
            case Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN:
                Log.INFO.log("Received message: %s", message);
            default:
                Log.ERROR.log("Unknown message: %s", message);
        }
    }

    public void dispatchEvent(String message) throws InterruptedException {
        final String[] tokens = message.split(" ");
        final String opt = tokens[0];
        switch (opt) {
            case Protocol.REMOVE_TOKEN:
                listenRemove(tokens[1]);
                break;
            default:
                Log.ERROR.log("Unknown message: %s", message);
        }
    }

    @Override
    public BlockingQueue<Map.Entry<Peer.InMessageType, String>> getInQueue() {
        return inQueue;
    }

    private void onList(List<String> fileLists) {
        AnyDoor.fileManager.updateFromDstore(fileLists, port);
        // TODO notify controller
    }

    private void onStoreAck(String filename) {
        Optional<FileHandler> _file = AnyDoor.fileManager.fetch(filename);
        DstoreHandler dstorePeer = AnyDoor.onlineDstores.get(port);
        if (_file.isEmpty() || dstorePeer == null) {
            Log.ERROR.log("File or Dstore not found: %s %s", filename, port);
            return;
        }
        FileHandler file = _file.get();
        if (file.getLock().tryLock()) {
            try {
                if (file.ackStore(dstorePeer)) {
                    try {
                        AnyDoor.onlineClients.forEach(client -> {
                            try {
                                client.getInQueue().put(new AbstractMap.SimpleImmutableEntry<>(
                                        Peer.InMessageType.EVENT,
                                        String.format("%s %s", Protocol.STORE_COMPLETE_TOKEN, filename)));
                            } catch (InterruptedException e) {
                                Log.ERROR.log("Error while broadcasting store complete message");
                            }
                        });
                    } catch (Exception e) {
                        Log.ERROR.log("Error while broadcasting store complete message");
                    }
                }
            } finally {
                file.getLock().unlock();
            }
        }
    }

    private void onRemoveAck(String filename) {
        Optional<FileHandler> _file = AnyDoor.fileManager.fetch(filename);
        DstoreHandler dstorePeer = AnyDoor.onlineDstores.get(port);
        if (_file.isEmpty() || dstorePeer == null) {
            Log.ERROR.log("File or Dstore not found: %s %s", filename, port);
            return;
        }
        FileHandler file = _file.get();
        if (file.getLock().tryLock()) {
            try {
                if (file.ackRemove(dstorePeer)) {
                    try {
                        file.getWaitingClient().getInQueue().put(new AbstractMap.SimpleImmutableEntry<>(
                                Peer.InMessageType.EVENT,
                                String.format("%s %s", Protocol.REMOVE_COMPLETE_TOKEN, filename)));
                    } catch (Exception e) {
                        Log.ERROR.log("Error while broadcasting remove complete message");
                    }
                }
            } finally {
                file.getLock().unlock();
            }
        }
    }

    private void listenRemove(String filename) throws InterruptedException {
        Log.TRACE.log("Received remove message: %s", filename);
        res(Protocol.REMOVE_TOKEN + " " + filename);
    }

    @Override
    public BlockingQueue<Map.Entry<Peer.OutMessageType, String>> getOutQueue() {
        return outQueue;
    }

    public Integer getPort() {
        return port;
    }
}

class ClientHandler implements Handler {

    private final BlockingQueue<Map.Entry<Peer.InMessageType, String>> inQueue;
    private final BlockingQueue<Map.Entry<Peer.OutMessageType, String>> outQueue;

    private final Map<FileHandler, Set<DstoreHandler>> latestLoadDstores = new HashMap<>();

    private AtomicReference<String> waitStoreFile = new AtomicReference<>();

    private AtomicReference<String> waitRemoveFile = new AtomicReference<>();

    public ClientHandler(BlockingQueue<Map.Entry<Peer.InMessageType, String>> inQueue, BlockingQueue<Map.Entry<Peer.OutMessageType, String>> outQueue) {
        this.inQueue = inQueue;
        this.outQueue = outQueue;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Map.Entry<Peer.InMessageType, String> res = inQueue.take();
                switch (res.getKey()) {
                    case REQ:
                        dispatchReq(res.getValue());
                        break;
                    case EVENT:
                        dispatchEvent(res.getValue());
                        break;
                }
                Log.INFO.log("Received message: %s", res);
            } catch (InterruptedException e) {
                Log.ERROR.log("Error while taking message from inQueue");
                break;
            }
        }
    }

    public void dispatchReq(String message) throws InterruptedException {
        final String[] tokens = message.split(" ");
        final String opt = tokens[0];
        switch (opt) {
            case Protocol.STORE_TOKEN:
                onClientStore(tokens[1], Long.parseLong(tokens[2]));
                break;
            case Protocol.LOAD_TOKEN:
            case Protocol.RELOAD_TOKEN:
                onClientLoad(tokens[1]);
                break;

            case Protocol.REMOVE_TOKEN:
                onClientRemove(tokens[1]);
                break;
            case Protocol.LIST_TOKEN:
                onList();
                break;
            default:
                Log.ERROR.log("Unknown message: %s", message);
        }
    }

    private void onClientStore(String filename, long fileSize) throws InterruptedException {
        Log.TRACE.log("Store request: %s %d", filename, fileSize);
        if (AnyDoor.onlineDstores.size() < AnyDoor.replicate) {
            res(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }
        if (AnyDoor.fileManager.contains(filename)) {
            Log.ERROR.log("File already exists: %s", filename);
            res(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
            return;
        }
        Optional<FileHandler> _file = AnyDoor.fileManager.startStoring(filename, fileSize);
        if (_file.isEmpty()) {
            Log.ERROR.log("File already exists: %s", filename);
            res(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
            return;
        }
        final List<DstoreHandler> candidateDstore = AnyDoor.onlineDstores.values().stream().limit(AnyDoor.replicate).toList();
        FileHandler file = _file.get();
        if (file.getLock().tryLock()) {
            try {
                file.store(this, candidateDstore);
                waitStoreFile.set(filename);
            } finally {
                file.getLock().unlock();
            }
        }
        res(Protocol.STORE_TO_TOKEN + " " + candidateDstore.stream().map(DstoreHandler::getPort).map(String::valueOf).collect(Collectors.joining(" ")));
    }

    private void onClientLoad(String fileName) throws InterruptedException {
        if (AnyDoor.onlineDstores.size() < AnyDoor.replicate) {
            res(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }
        Optional<FileHandler> _file = AnyDoor.fileManager.fetch(fileName);
        if (_file.isEmpty()) {
            Log.ERROR.log("File does not exist: %s", fileName);
            res(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            return;
        }
        FileHandler file = _file.get();
        Optional<DstoreHandler> _dstorePeer = file.getOnDstores().stream()
                .filter(dstore -> Optional.ofNullable(latestLoadDstores.get(file)).map(ds -> !ds.contains(dstore)).orElse(true))
                .findAny();
        if (_dstorePeer.isEmpty()) {
            latestLoadDstores.clear();
            res(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            return;
        }
        latestLoadDstores.putIfAbsent(file, new HashSet<>());
        latestLoadDstores.get(file).add(_dstorePeer.get());
        res(String.format("%s %s %s", Protocol.LOAD_FROM_TOKEN, _dstorePeer.get().getPort(), file.getFileSize()));
    }

    private void onClientRemove(String filename) throws InterruptedException {
        if (AnyDoor.onlineDstores.size() < AnyDoor.replicate) {
            res(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }
        Optional<FileHandler> _file = AnyDoor.fileManager.fetch(filename);
        if (_file.isEmpty()) {
            res(Protocol.REMOVE_COMPLETE_TOKEN);
            return;
        }
        FileHandler file = _file.get();
        if (file.getLock().tryLock()) {
            try {
                file.getStatus().set(2);
                waitRemoveFile.set(filename);
                try {
                    file.getOnDstores().forEach(dstore -> {
                        try {
                            dstore.getInQueue().put(new AbstractMap.SimpleImmutableEntry<>(
                                    Peer.InMessageType.EVENT,
                                    String.format("%s %s", Protocol.REMOVE_TOKEN, filename)));
                        } catch (InterruptedException e) {
                            Log.ERROR.log("Error while broadcasting remove message");
                        }
                    });
                } catch (Exception e) {
                    Log.ERROR.log("Error while broadcasting remove message");
                }
            } finally {
                file.getLock().unlock();
            }
        }
    }

    private void onList() throws InterruptedException {
        if (AnyDoor.onlineDstores.size() < AnyDoor.replicate) {
            Log.ERROR.log("No Dstores available");
            res(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }
        List<String> fileList = AnyDoor.fileManager.list();
        if (fileList.isEmpty()) {
            res(Protocol.LIST_TOKEN);
        } else {
            res(Protocol.LIST_TOKEN + " " + String.join(" ", fileList));
        }
    }

    public void dispatchEvent(String message) {
        final String[] tokens = message.split(" ");
        final String opt = tokens[0];
        switch (opt) {
            case Protocol.STORE_COMPLETE_TOKEN:
                listenStoreComplete(tokens[1]);
                break;
            case Protocol.REMOVE_COMPLETE_TOKEN:
                listenRemoveComplete(tokens[1]);
                break;
            default:
                Log.ERROR.log("Unknown message: %s", message);
        }
    }

    @Override
    public BlockingQueue<Map.Entry<Peer.InMessageType, String>> getInQueue() {
        return inQueue;
    }

    private void listenStoreComplete(String filename) {
        if (waitStoreFile.get().equals(filename)) {
            waitStoreFile.set(null);
            try {
                res(Protocol.STORE_COMPLETE_TOKEN);
            } catch (InterruptedException e) {
                Log.ERROR.log("Error while sending store complete message");
            }
        }
    }

    private void listenRemoveComplete(String filename) {
        if (waitRemoveFile.get().equals(filename)) {
            waitRemoveFile.set(null);
            try {
                res(Protocol.REMOVE_COMPLETE_TOKEN);
            } catch (InterruptedException e) {
                Log.ERROR.log("Error while sending remove complete message");
            }
        }
    }

    @Override
    public BlockingQueue<Map.Entry<Peer.OutMessageType, String>> getOutQueue() {
        return outQueue;
    }
}

class FileHandler {

    private final String fileName;

    private final long fileSize;

    private final long addedTime = System.currentTimeMillis();

    private final List<DstoreHandler> onDstores = new CopyOnWriteArrayList<>();

    private final Set<DstoreHandler> waitingDstores = new CopyOnWriteArraySet<>();
    /**
     * mutex lock
     */
    private final Lock lock = new ReentrantLock();
    /**
     * 0: not stored 1 : stored 2 : removing
     */
    private final AtomicInteger status = new AtomicInteger(0);
    private ClientHandler waitingClient;

    public FileHandler(String fileName, long fileSize) {
        this.fileName = fileName;
        this.fileSize = fileSize;
    }

    public String getFileName() {
        return fileName;
    }

    public Lock getLock() {
        return lock;
    }

    public List<DstoreHandler> getOnDstores() {
        return onDstores;
    }

    public AtomicInteger getStatus() {
        return status;
    }

    public long getFileSize() {
        return fileSize;
    }

    public boolean ackStore(DstoreHandler dstore) {
        waitingDstores.remove(dstore);
        onDstores.add(dstore);
        if (waitingDstores.isEmpty()) {
            status.set(1);
            return true;
        }
        return false;
    }

    public boolean ackRemove(DstoreHandler dstore) {
        waitingDstores.remove(dstore);
        onDstores.remove(dstore);
        if (onDstores.isEmpty()) {
            status.set(2);
            return true;
        }
        return false;
    }

    public boolean expired(long now) {
        if (this.status.intValue() == 1) {
            return false;
        }
        return now - addedTime > AnyDoor.timeout;
    }

    public boolean visible() {
        return status.get() == 1;
    }

    public void removeDstore(DstoreHandler dstorePeer) {
        onDstores.remove(dstorePeer);
        waitingDstores.remove(dstorePeer);
    }

    public void store(ClientHandler clientHandler, List<DstoreHandler> candidateDstore) {
        this.waitingClient = clientHandler;
        this.waitingDstores.addAll(candidateDstore);
    }

    public ClientHandler getWaitingClient() {
        return waitingClient;
    }
}
//
//class RebalanceManager {
//
//
//    public boolean startRebalance(String source) {
//        Log.INFO.log("Rebalancing from %s", source)
//        ;
//        if (AnyDoor.onlineDstores.size() < AnyDoor.replicate) {
//            Log.WARN.log("not enough dstore to rebalance");
//            return false;
//        }
//        final List<DstoreHandler> dstoreList = AnyDoor.onlineDstores.values().stream().toList();
//
//        // request all dstore to list files
//        for (DstoreHandler dstoreHandler : dstoreList) {
//            BlockingQueue<Map.Entry<Peer.InMessageType, String>> out = dstoreHandler.getInQueue();
//            try {
//                out.put(new AbstractMap.SimpleImmutableEntry<>(Peer.InMessageType.REQ, Protocol.LIST_TOKEN));
//            } catch (InterruptedException e) {
//                Log.ERROR.log("Error while sending list request");
//                return false;
//            }
//        }
//
//        final List<FileHandler> allFiles = AnyDoor.fileManager.listFiles();
//        final Map<String, List<DstoreHandler>> fileDstoreMap = allFiles.stream().collect(Collectors.toMap(FileHandler::getFileName, FileHandler::getOnDstores));
//
//        final Map<DstoreHandler, List<FileHandler>> dstoreFileMap = new HashMap<>();
//        for (FileHandler file : allFiles) {
//            for (DstoreHandler dstoreHandler : file.getOnDstores()) {
//                dstoreFileMap.putIfAbsent(dstoreHandler, new ArrayList<>());
//                dstoreFileMap.get(dstoreHandler).add(file);
//            }
//        }
//
//        final int fileCount = allFiles.size();
//        final int dstoreCount = dstoreList.size();
//        final double targetCapacity = (double) (AnyDoor.replicate * fileCount) / dstoreCount;
//        final int lowerBound = (int) Math.floor(targetCapacity);
//        final int upperBound = (int) Math.ceil(targetCapacity);
//
//        // Determine which files need to be moved from overloaded to underloaded Dstores
//        final Map<DstoreHandler, Set<String>> dstoreRemoveMap = new HashMap<>();
//        final Map<DstoreHandler, Set<String>> dstoreAddMap = new HashMap<>();
//
////        // check each file
////        for (Map.Entry<String, List<DstoreHandler>> entry : fileDstoreMap.entrySet()) {
////            String file = entry.getKey();
////            List<DstoreHandler> holders = entry.getValue();
////
////            final Optional<FileHandler> _file = AnyDoor.fileManager.fetch(file);
////            if (_file.isEmpty()) {
////                continue;
////            }
////            FileHandler fileHandler = _file.get();
////            if (fileHandler.getStatus().intValue() == 2) {
////                fileHandler.getOnDstores().forEach(d -> dstoreRemoveMap.computeIfAbsent(d, data -> new HashSet<>()).add(file));
////                continue;
////            }
////
////            if (holders.size() < AnyDoor.replicate) {
////                // file needs more replicas
////                final List<DstoreHandler> underloaded = dstoreFileMap.entrySet().stream()
////                        .filter(e -> !holders.contains(e.getKey()))
////                        .filter(e -> e.getValue().size() < upperBound)
////                        .limit(AnyDoor.replicate - holders.size())
////                        .map(Map.Entry::getKey)
////                        .collect(Collectors.toList());
////
////                if (holders.size() + underloaded.size() <= AnyDoor.replicate) {
////                    dstoreList.stream()
////                            .filter(d -> !holders.contains(d) && !underloaded.contains(d))
////                            .limit(AnyDoor.replicate - holders.size() - underloaded.size())
////                            .forEach(underloaded::add);
////                }
////                underloaded.forEach(d -> dstoreAddMap.computeIfAbsent(d, data -> new HashSet<>()).add(file));
////            } else if (holders.size() > AnyDoor.replicate) {
////                // File has too many replicas
////                holders.stream()
////                        .map(h -> Map.entry(h, dstoreFileMap.getOrDefault(h, Collections.emptyList())))
////                        .filter(e -> e.getValue().size() > lowerBound)
////                        .map(Map.Entry::getKey)
////                        .limit(holders.size() - AnyDoor.replicate)
////                        .forEach(d -> dstoreRemoveMap.computeIfAbsent(d, data -> new HashSet<>()).add(file));
////            }
////        }
//
//        // calc expected load:  Dstore -> file count
//        Map<DstoreHandler, Integer> loadMap = new HashMap<>();
//        for (DstoreHandler dstore : dstoreList) {
//            int initialLoad = Optional.ofNullable(dstoreFileMap.get(dstore)).map(List::size).orElse(0);
//            int additions = dstoreAddMap.getOrDefault(dstore, Collections.emptySet()).size();
//            int removals = dstoreRemoveMap.getOrDefault(dstore, Collections.emptySet()).size();
//            int netLoad = initialLoad + additions - removals;
//            loadMap.put(dstore, netLoad);
//        }
//
//        // find high load and low load Dstore List
//        List<DstoreHandler> highLoadDstores = loadMap.entrySet().stream()
//                .filter(entry -> entry.getValue() > upperBound)
//                .map(Map.Entry::getKey)
//                .toList();
//        List<DstoreHandler> lowLoadDstores = loadMap.entrySet().stream()
//                .filter(entry -> entry.getValue() < lowerBound)
//                .map(Map.Entry::getKey)
//                .toList();
//
//        // transfer files from high load Dstore to low load Dstore
//        for (DstoreHandler highLoadDstore : highLoadDstores) {
//            final Set<String> potentialFiles = dstoreFileMap.getOrDefault(highLoadDstore, Collections.emptyList())
//                    .stream()
//                    .map(FileHandler::getFileName)
//                    .collect(Collectors.toSet());
//            potentialFiles.removeAll(dstoreRemoveMap.getOrDefault(highLoadDstore, Collections.emptySet()));
//
//            for (String file : potentialFiles) {
//                if (loadMap.get(highLoadDstore) > upperBound) {
//                    for (DstoreHandler lowLoadDstore : lowLoadDstores) {
//                        if (loadMap.get(lowLoadDstore) < lowerBound) {
//                            dstoreAddMap.computeIfAbsent(lowLoadDstore, k -> new HashSet<>()).add(file);
//                            dstoreRemoveMap.computeIfAbsent(highLoadDstore, k -> new HashSet<>()).add(file);
//
//                            // update load
//                            loadMap.put(highLoadDstore, loadMap.get(highLoadDstore) - 1);
//                            loadMap.put(lowLoadDstore, loadMap.get(lowLoadDstore) + 1);
//                            break;
//                        }
//                    }
//                }
//            }
//        }
//
//        // execute removals and additions
//        final Set<String> addFiles = dstoreAddMap.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
//        for (DstoreHandler dstore : dstoreList) {
//            final List<String> canSendToOthers = dstore.fileList().stream()
//                    .filter(addFiles::contains)
//                    .toList();
//            final StringBuilder sendFileCmd = new StringBuilder();
//            for (String file : canSendToOthers) {
//                List<DstoreHandler> peersDstore = dstoreAddMap.entrySet().stream().filter(e -> e.getValue().contains(file)).map(Map.Entry::getKey).toList();
//                if (peersDstore.isEmpty()) {
//                    continue;
//                }
//                sendFileCmd.append(" ").append(file).append(" ").append(peersDstore.size());
//                peersDstore.forEach(peer -> sendFileCmd.append(" ").append(peer.port()));
//            }
//
//            Set<String> removeFiles = dstoreRemoveMap.getOrDefault(dstore, Collections.emptySet());
//
//            StringBuilder rebalanceRequest = new StringBuilder();
//            rebalanceRequest.append(Protocol.REBALANCE_TOKEN).append(" ").append(canSendToOthers.size());
//            if (!canSendToOthers.isEmpty()) {
//                rebalanceRequest.append(sendFileCmd);
//            }
//            rebalanceRequest.append(" ").append(removeFiles.size());
//            if (!removeFiles.isEmpty()) {
//                rebalanceRequest.append(" ").append(String.join(" ", removeFiles));
//            }
//
//
//            dstore.out().println(rebalanceRequest);
//            dstore.fileList().removeAll(removeFiles);
//            dstore.fileList().addAll(canSendToOthers);
//        }
//        return false;
//    }
//
//}