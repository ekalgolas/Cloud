package master.dht.nio.server;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadPool {

    private static ExecutorService es = Executors.newFixedThreadPool(10);

    public static void execute(Runnable thread) {
        es.execute(thread);
        // new Thread(thread).start();
    }
}
