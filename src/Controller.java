import java.io.*;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Controller {


}

class AnyDoor {

    Log logLevel = Log.INFO;

}

class ClientPeer {



}

class DstorePeer {

}

class Peer {

    private final Socket socket;

    private final BufferedReader in;

    private final PrintWriter out;

    private final BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();

    public Peer(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);

    }

    public void receiveMessage() {
        try {
            String message = in.readLine();
            messageQueue.put(message);
        } catch (IOException | InterruptedException e) {

        }
    }
}

enum Log {
    TRACE,
    DEBUG,
    INFO,
    ERROR
    ;

    private static final DateTimeFormatter dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public void log(String msg, Object... args) {
        if (msg.endsWith("\n")) {
            msg = msg.substring(0, msg.length() - 1);
        }

        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        final String currentCall = stackTrace.length > 3 ? stackTrace[3].toString() : "-";
        System.out.printf("%s [%s] %s %s\n", LocalDateTime.now().format(dateTimeFormat), this.name(), currentCall, String.format(msg, args));
    }
}