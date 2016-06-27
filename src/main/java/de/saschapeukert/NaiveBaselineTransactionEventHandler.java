package de.saschapeukert;

import org.neo4j.graphdb.*;
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
        // RELS
        Iterator<Relationship> rit = transactionData.createdRelationships().iterator();
        while (rit.hasNext()) {
            Relationship r = rit.next();
            if(entityHasProperty(r,BaselineProcedure.PROPERTYKEY)){
                try (Transaction tx = db.beginTx()) {
                    r.delete();
                    tx.success();
                }
            }
        }

        // NODES
        Iterator<Node> it = transactionData.createdNodes().iterator();
        while (it.hasNext()) {
            Node n = it.next();
            if(entityHasProperty(n,BaselineProcedure.PROPERTYKEY)){
                try (Transaction tx = db.beginTx()) {
                    n.delete();
                    tx.success();
                }
            }
        }
        return null;
    }

    private boolean entityHasProperty(PropertyContainer entity, String key){
        // would like to delete it here too, but can't because of PropertyContainer
        if(entity.hasProperty(key)){
            return true;
        }
        return false;
    }

    @Override
    public void afterCommit(TransactionData transactionData, Object o) {
        return;
    }

    @Override
    public void afterRollback(TransactionData transactionData, Object o) {}
}
