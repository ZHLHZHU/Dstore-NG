import java.io.File;
import java.io.IOException;

public class TestNewNewNew {

    public static void main(String[] args) throws IOException, InterruptedException {
        var client = new Client(10250, 1000, Logger.LoggingType.ON_FILE_AND_TERMINAL);
        client.connect();
        System.out.println("[Client] Connecting...");

        final String[] list = client.list();
        assert list.length == 0;

        client.store(new File("to_store/1.txt"));

        // wait input...
        System.out.println("Press Enter to continue...");
        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }

//        final String[] list1 = client.list();
//        assert list1.length == 1;
//
//        byte[] res = client.load("1.txt");
//        // write to file
//        File file = new File("output-new.txt");
//        try (FileOutputStream fos = new FileOutputStream(file)) {
//            fos.write(res);
//            fos.flush();  // Ensure all data is written to the file system
//        } catch (IOException e) {
//            e.printStackTrace();  // Handle exceptions, possibly logging them
//        }

//        client.remove("1.txt");

        var t1 = new Thread(() -> {
            try {
                var c1 = new Client(10250, 1000, Logger.LoggingType.ON_FILE_AND_TERMINAL);
                c1.connect();
                c1.remove("1.txt");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        var t2 = new Thread(() -> {
            try {
                var c2 = new Client(10250, 1000, Logger.LoggingType.ON_FILE_AND_TERMINAL);
                c2.connect();
                c2.remove("1.txt");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        var t3 = new Thread(() -> {
            try {
                var c3 = new Client(10250, 1000, Logger.LoggingType.ON_FILE_AND_TERMINAL);
                c3.connect();
                c3.remove("1.txt");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();

        final String[] list1 = client.list();
        assert list1.length == 0;
//        assert list2.length == 0;

        client.disconnect();


    }
}
