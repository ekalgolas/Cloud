package master.dht.dhtfs.server.datanode;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionManager {

    private TransactionIdAssigner idAssigner;
    private Map<String, Transaction> transactions;

    public TransactionManager() {
        idAssigner = new TransactionIdAssigner();
        transactions = new ConcurrentHashMap<String, Transaction>();
    }

    public Transaction getTransaction(String transactionId) {
        return transactions.get(transactionId);
    }

    public String addTransaction(Transaction transaction) throws IOException {
        String id = idAssigner.generateUID();
        transaction.setTransactionId(id);
        transactions.put(id, transaction);
        return id;
    }

    public void removeTransaction(String transactionId) throws IOException {
        Transaction tran = transactions.remove(transactionId);
        tran.release();
    }
}
