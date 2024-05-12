import java.io.*;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Controller {


}

class AnyDoor {

    public static Log logLevel = Log.INFO;

    public static long timeout = 1000;

    public static final List<DstorePeer> onlineDstores = new CopyOnWriteArrayList<>();

    public static final AtomicInteger replicate = new AtomicInteger(0);

    public static final FileManager fileManager = new FileManager();

}


class EventManager {

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
        if (file == null || file.visible() || file.expired()) {
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
}

class Peer {

    private final Socket socket;

    private final BufferedReader in;

    private final PrintWriter out;

    public final BlockingQueue<Map.Entry<InMessageType, String>> inQueue = new LinkedBlockingQueue<>();
    protected final BlockingQueue<Map.Entry<OutMessageType, String>> outQueue = new LinkedBlockingQueue<>();

    public Peer(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
    }

    public void receiveMessage() {
        try {
            String message = in.readLine();
            inQueue.put(new AbstractMap.SimpleImmutableEntry<>(InMessageType.REQ, message));
        } catch (IOException | InterruptedException e) {
            Log.ERROR.log("Error while reading message from socket");
        }
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

class DstorePeer extends Peer {

    private final Integer port;

    public DstorePeer(Socket socket, int port) throws IOException {
        super(socket);
        this.port = port;
        Thread handler = new Thread(new DstoreHandler(inQueue, outQueue));
        handler.start();
    }

    public Integer getPort() {
        return port;
    }
}

class DstoreHandler implements Runnable {

    private final BlockingQueue<Map.Entry<Peer.InMessageType, String>> inQueue;
    private final BlockingQueue<Map.Entry<Peer.OutMessageType, String>> outQueue;

    public DstoreHandler(BlockingQueue<Map.Entry<Peer.InMessageType, String>> inQueue, BlockingQueue<Map.Entry<Peer.OutMessageType, String>> outQueue) {
        this.inQueue = inQueue;
        this.outQueue = outQueue;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Map.Entry<Peer.InMessageType, String> res = inQueue.take();
                Log.INFO.log("Received message: %s", res);
            } catch (InterruptedException e) {
                Log.ERROR.log("Error while taking message from inQueue");
            }
        }
    }


    private void onStoreAck(String filename) {

    }

}


class ClientPeer extends Peer {

    public ClientPeer(Socket socket) throws IOException {
        super(socket);
    }
}

class ClientHandler implements Runnable, Exchangeable {

    private final BlockingQueue<Map.Entry<Peer.InMessageType, String>> inQueue;
    private final BlockingQueue<Map.Entry<Peer.OutMessageType, String>> outQueue;

    private final Map<FileHandler, Set<DstorePeer>> latestLoadDstores = new HashMap<>();

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
                        broadcast(res.getValue());
                        break;
                }
                Log.INFO.log("Received message: %s", res);
            } catch (InterruptedException e) {
                Log.ERROR.log("Error while taking message from inQueue");
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
        if (AnyDoor.onlineDstores.size() < AnyDoor.replicate.intValue()) {
            Log.ERROR.log("No Dstores available");
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
        FileHandler file = _file.get();
        if (file.getLock().tryLock()) {
            try {
                file.getWaitingDstores().addAll(AnyDoor.onlineDstores.stream().limit(AnyDoor.replicate.intValue()).toList());
                waitStoreFile.set(filename);
            } finally {
                file.getLock().unlock();
            }
        }
    }

    private void onClientLoad(String fileName) throws InterruptedException {
        if (AnyDoor.onlineDstores.size() < AnyDoor.replicate.intValue()) {
            Log.ERROR.log("No Dstores available");
            res(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }
        Optional<FileHandler> _file = AnyDoor.fileManager.fetch(fileName);
        if (_file.isEmpty()) {
            Log.ERROR.log("File does not exist: %s", fileName);
            res(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        }
        FileHandler file = _file.get();
        Optional<DstorePeer> _dstorePeer = file.getOnDstores().stream()
                .filter(dstore -> !latestLoadDstores.get(file).contains(dstore))
                .findAny();
        if (_dstorePeer.isEmpty()) {
            latestLoadDstores.clear();
            res(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            return;
        }
        res(String.format("%s %s %s", Protocol.LOAD_FROM_TOKEN, _dstorePeer.get().getPort(), file.getFileSize()));
    }

    private void onClientRemove(String filename) throws InterruptedException {
        if (AnyDoor.onlineDstores.size() < AnyDoor.replicate.intValue()) {
            Log.ERROR.log("No Dstores available");
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
                            dstore.inQueue.put(new AbstractMap.SimpleImmutableEntry<>(
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
        if (AnyDoor.onlineDstores.size() < AnyDoor.replicate.intValue()) {
            Log.ERROR.log("No Dstores available");
            res(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }
        List<String> fileList = AnyDoor.fileManager.list();
        if (fileList.isEmpty()) {
            res(Protocol.LIST_TOKEN);
        } else {
            res(String.join("%s ", Protocol.LIST_TOKEN, String.join(" ", fileList)));
        }
    }

    private void dispatchEvent(String message) {
        final String[] tokens = message.split(" ");
        final String opt = tokens[0];
        switch (opt) {
            case Protocol.STORE_ACK_TOKEN:
                onStoreAck(tokens[1]);
                break;
            default:
                Log.ERROR.log("Unknown message: %s", message);
        }
    }

    @Override
    public BlockingQueue<Map.Entry<Peer.OutMessageType, String>> getOutQueue() {
        return outQueue;
    }
}

enum Log {
    TRACE,
    DEBUG,
    INFO,
    ERROR;

    private static final DateTimeFormatter dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public void log(String msg, Object... args) {
        if (this.ordinal() < AnyDoor.logLevel.ordinal()) {
            return;
        }

        if (msg.endsWith("\n")) {
            msg = msg.substring(0, msg.length() - 1);
        }

        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        final String currentCall = stackTrace.length > 3 ? stackTrace[3].toString() : "-";
        System.out.printf("%s [%s] %s %s\n", LocalDateTime.now().format(dateTimeFormat), this.name(), currentCall, String.format(msg, args));
    }
}

class FileHandler {

    private final String fileName;

    private final long fileSize;

    private final long addedTime = System.currentTimeMillis();

    private final List<DstorePeer> onDstores = new CopyOnWriteArrayList<>();

    private final List<DstorePeer> waitingDstores = new CopyOnWriteArrayList<>();

    /**
     * mutex lock
     */
    private final Lock lock = new ReentrantLock();

    /**
     * 0: not stored 1 : stored 2 : removing
     */
    private final AtomicInteger status = new AtomicInteger(0);

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

    public List<DstorePeer> getOnDstores() {
        return onDstores;
    }

    public AtomicInteger getStatus() {
        return status;
    }

    public List<DstorePeer> getWaitingDstores() {
        return waitingDstores;
    }

    public long getFileSize() {
        return fileSize;
    }

    public boolean ack(DstorePeer dstore) {
        waitingDstores.remove(dstore);
        onDstores.add(dstore);
        if (waitingDstores.isEmpty()) {
            status.set(1);
            return true;
        }
        return false;
    }

    public boolean expired() {
        return System.currentTimeMillis() - addedTime > AnyDoor.timeout;
    }

    public boolean visible() {
        return status.get() == 1;
    }
}

interface Exchangeable {

    default void res(String message) throws InterruptedException {
        getOutQueue().put(new AbstractMap.SimpleImmutableEntry<>(Peer.OutMessageType.RES, message));
    }

    default void broadcast(String message) throws InterruptedException {
        getOutQueue().put(new AbstractMap.SimpleImmutableEntry<>(Peer.OutMessageType.BROADCAST, message));
    }

    BlockingQueue<Map.Entry<Peer.OutMessageType, String>> getOutQueue();

}