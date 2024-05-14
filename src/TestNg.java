import java.io.File;
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

        client.remove("1.jpg");

        final String[] list2 = client.list();
        assert list2.length == 0;

        client.disconnect();
    }
}
