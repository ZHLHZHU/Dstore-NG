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
        client.store(new File("to_store/1.txt"));
        client.store(new File("to_store/2.txt"));
//        client.store(new File("to_store/3.txt"));
//        client.store(new File("to_store/2.jpg"));

//        Thread.sleep(30000);

        final String[] list1 = client.list();
        assert list1.length == 4;
        System.out.println("List of files:");
        for (String s : list1) {
            System.out.println(s);
        }


        client.disconnect();
    }
}
