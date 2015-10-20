package master.dht.dhtfs.server.datanode;

import java.io.IOException;

public class Transaction {
    private String transactionId;

    private TransactionType transactionType;

    public Transaction(TransactionType transactionType) {
        this.setTransactionType(transactionType);
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(TransactionType transactionType) {
        this.transactionType = transactionType;
    }

    public void release() throws IOException {

    }

}
