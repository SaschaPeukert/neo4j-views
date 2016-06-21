package de.saschapeukert;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Iterator;

/**
 * Created by Sascha Peukert on 14.06.2016.
 */
public class NaiveBaselineTransactionEventHandler implements TransactionEventHandler {

    GraphDatabaseAPI db;

    public void setGraphDB(GraphDatabaseAPI gdb){
        db = gdb;
    }

    @Override
    public Object beforeCommit(TransactionData transactionData) throws Exception {
        // if new node has got certain label it needs to be removed
        try (Transaction tx = db.beginTx()) {
            Iterator<Relationship> rit = transactionData.createdRelationships().iterator();
            while (rit.hasNext()) {
                rit.next().delete();
            }

            Iterator<Node> it = transactionData.createdNodes().iterator();
            while (it.hasNext()) {
                Node n = it.next();
                // TESTING IN PROGRESS
                //if(n.hasLabel(Label.label(BaselineProcedure.LABELNAME))){
                // detatch delete
                n.delete();
            }
            tx.success();
        }
        return null;
    }

    @Override
    public void afterCommit(TransactionData transactionData, Object o) {
        return;
    }

    @Override
    public void afterRollback(TransactionData transactionData, Object o) {}
}
