package de.saschapeukert;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
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
        // RELS
        Iterator<Relationship> rit = transactionData.createdRelationships().iterator();
        while (rit.hasNext()) {
            Relationship r = rit.next();
            if(deleteRelationshipIfPropertyMatches(r,BaselineProcedure.PROPERTYKEY)){
                continue;
            }

            // Also: Remove non-virtual REL if it is connected to virtual Node
            // this could happen with additional MERGE clause
            Node[] nodes =r.getNodes();
            for (Node n : nodes){
                if(entityHasPropertySetTrue(n,BaselineProcedure.PROPERTYKEY) &&
                        !entityHasPropertySetTrue(r,BaselineProcedure.PROPERTYKEY)){
                    try (Transaction tx = db.beginTx()) {
                        r.delete();
                        tx.success();
                        continue;
                    }
                }
            }
        }

        // NODES
        Iterator<Node> it = transactionData.createdNodes().iterator();
        while (it.hasNext()) {
            Node n = it.next();
            deleteNodeIfPropertyMatches(n,BaselineProcedure.PROPERTYKEY);
        }
        return null;
    }

    private boolean deleteNodeIfPropertyMatches(Node entity, String key){
        if(entityHasPropertySetTrue(entity,key)){
            try (Transaction tx = db.beginTx()) {
                entity.delete();
                tx.success();
                return true;
            }
        }
        return false;
    }

    private boolean deleteRelationshipIfPropertyMatches(Relationship entity, String key){
        if(entityHasPropertySetTrue(entity,key)){
            try (Transaction tx = db.beginTx()) {
                entity.delete();
                tx.success();
                return true;
            }
        }
        return false;
    }
    private boolean entityHasPropertySetTrue(PropertyContainer entity, String key){
        // would like to delete it here too, but can't because of PropertyContainer
        if(entity.hasProperty(key)){
            try {
                boolean isSet = (boolean) entity.getProperty(key);
                if (isSet)
                    return true;
            } catch (Exception e){
                // if, for whatever reason, this property is not a boolean
                return false;
            }
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
