import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class TestNg {

    public static void main(String[] args) throws IOException, InterruptedException {
        var client = new Client(10250, 1000, Logger.LoggingType.ON_FILE_AND_TERMINAL);
        client.connect();
        System.out.println("[Client] Connecting...");

        final String[] list = client.list();
        assert list.length == 0;

        client.store(new File("to_store/1.jpg"));

        final String[] list1 = client.list();
        assert list1.length == 1;

        byte[] res = client.load("1.jpg");
        // write to file
        File file = new File("output.jpg");
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(res);
            fos.flush();  // Ensure all data is written to the file system
        } catch (IOException e) {
            e.printStackTrace();  // Handle exceptions, possibly logging them
        }

        client.remove("1.jpg");

        final String[] list2 = client.list();
        assert list2.length == 0;

        client.disconnect();
    }
}
