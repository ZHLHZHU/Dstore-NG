import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class TestNgNew {

    public static void main(String[] args) throws IOException {
        var client = new Client(10250, 1000, Logger.LoggingType.ON_FILE_AND_TERMINAL);
        client.connect();
        System.out.println("[Client] Connecting...");

        final String[] list = client.list();
        assert list.length == 0;

        client.store(new File("to_store/1.jpg"));

        try {
            client.store(new File("to_store/1.jpg"));
        } catch (IOException e) {
            System.out.println("!!! Error: " + e.getMessage());
            System.out.println("Don't panic! This is expected.");
        }

        final String[] list1 = client.list();
        assert list1.length == 1;

        byte[] res = client.load("1.jpg");
        // write to file
        File file = new File("output.txt");
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(res);
            fos.flush();  // Ensure all data is written to the file system
        } catch (IOException e) {
            e.printStackTrace();  // Handle exceptions, possibly logging them
        }

        try {
            client.wrongLoad("1.jpg", 2);
        } catch (IOException e) {
            System.out.println("!!! Error: " + e.getMessage());
            System.out.println("Don't panic! This is expected.");
        }

        client.remove("1.jpg");
        final String[] list2 = client.list();
        assert list2.length == 0;

        try {
            client.wrongStore("1.jpg", new byte[] {1, 2, 3});
        } catch (IOException e) {
            System.out.println("!!! Error: " + e.getMessage());
            System.out.println("Don't panic! This is expected.");
        }

        try {
            client.load("nonexistent.txt");
        } catch (IOException e) {
            System.out.println("!!! Error: " + e.getMessage());
            System.out.println("Don't panic! This is expected.");
        }

        try {
            client.remove("nonexistent.txt");
        } catch (IOException e) {
            System.out.println("!!! Error: " + e.getMessage());
            System.out.println("Don't panic! This is expected.");
        }

    }
}
