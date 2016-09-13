package de.saschapeukert;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by Sascha Peukert on 14.06.2016.
 */
public class BaselineProcedureTest {

    private static GraphDatabaseService db;

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig(GraphDatabaseSettings.load_csv_file_url_root,new File("src/test/resources").getAbsolutePath())
                .newGraphDatabase();
        registerProcedure(db, BaselineProcedure.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testGetMethodForPropertyKey() {
        Result r;
        try (Transaction tx = db.beginTx()) {
            r = db.execute("CALL de.saschapeukert.getVirtualPropertyKey()");
            tx.success();
        }
        Assert.assertNotNull("Result should not be null", r);
        while (r.hasNext()) {
            Map<String, Object> map = r.next();
            Set<String> set = map.keySet();
            Iterator<String> sit = set.iterator();

            String result = map.get(sit.next()).toString();
            Assert.assertEquals(BaselineProcedure.PROPERTYKEY, result);
        }
    }

    @Test
    public void MergeVirtualSetFalseShouldWorkInSameQuery() {
        Result r;
        try (Transaction tx = db.beginTx()) {
            r = db.execute("CALL de.saschapeukert.runCypher(\"CREATE VIRTUAL (n:Test), (m:Person) " +
                    " MERGE (n)<-[w:WROTE]-(m) ON CREATE SET n." + BaselineProcedure.PROPERTYKEY + "=false, " +
                    "m."+ BaselineProcedure.PROPERTYKEY + "=false " +
                    "RETURN Labels(n), Labels(m), Type(w)\", null)");
            tx.success();
        }
        Assert.assertNotNull("Result should not be null", r);
        while (r.hasNext()) {
            Map<String, Object> map = r.next();
            Set<String> set = map.keySet();
            Iterator<String> sit = set.iterator();

            String result = map.get(sit.next()).toString();
            String[] split = result.split(",");

            Assert.assertEquals("{Labels(m)=[Person]",split[0]);
            Assert.assertEquals(" Labels(n)=[Test]",split[1]);
            Assert.assertEquals(" Type(w)=WROTE}",split[2]);

            Assert.assertEquals(2,countAllNodes());
            Assert.assertEquals(1,countAllRelationships());
        }
        r.close();

        //clean up
        detachDeleteEverything();
    }

    @Test
    public void MergeVirtualSetFalseShouldWorkTransactionWide() {
        Result r;
        try (Transaction tx = db.beginTx()) {
            Result re = db.execute("CALL de.saschapeukert.runCypher(\"CREATE VIRTUAL (n:Test), (m:Person) " +
                    "\", null)");
            Assert.assertNotNull("Result should not be null", re);
            r = db.execute("CALL de.saschapeukert.runCypher(\"MERGE (n:Test)<-[w:WROTE]-(m:Person) ON MATCH SET n." +
                    BaselineProcedure.PROPERTYKEY + "=false, " +
                    "m."+ BaselineProcedure.PROPERTYKEY + "=false " +
                    " RETURN Labels(n), Labels(m), Type(w)" +
                    "\", null)");

            tx.success();
        }
        Assert.assertNotNull("Result should not be null", r);
        while (r.hasNext()) {
            Map<String, Object> map = r.next();
            Set<String> set = map.keySet();
            Iterator<String> sit = set.iterator();

            String result = map.get(sit.next()).toString();
            String[] split = result.split(",");

            Assert.assertEquals("{Labels(m)=[Person]",split[0]);
            Assert.assertEquals(" Labels(n)=[Test]",split[1]);
            Assert.assertEquals(" Type(w)=WROTE}",split[2]);

            Assert.assertEquals(2,countAllNodes());
            Assert.assertEquals(1,countAllRelationships());
        }

        r.close();

        //clean up
        detachDeleteEverything();
    }

    @Test
    public void virtualSetFalseShouldWorkTransactionWide() {
        Result r;
        try (Transaction tx = db.beginTx()) {
            Result re = db.execute("CALL de.saschapeukert.runCypher(\"CREATE VIRTUAL (n:Test)<-[:WROTE]-(m:Person) " +
                    "\", null)");
            Assert.assertNotNull("Result should not be null", re);
            r = db.execute("CALL de.saschapeukert.runCypher(\"MATCH (n:Test)<-[w:WROTE]-(m:Person) SET n." +
                    BaselineProcedure.PROPERTYKEY + "=false, " +
                    "m."+ BaselineProcedure.PROPERTYKEY + "=false " +
                    " RETURN Labels(n), Labels(m), Type(w)" +
                    "\", null)");

            tx.success();
        }
        Assert.assertNotNull("Result should not be null", r);
        while (r.hasNext()) {
            Map<String, Object> map = r.next();
            Set<String> set = map.keySet();
            Iterator<String> sit = set.iterator();

            String result = map.get(sit.next()).toString();
            String[] split = result.split(",");

            Assert.assertEquals("{Labels(m)=[Person]",split[0]);
            Assert.assertEquals(" Labels(n)=[Test]",split[1]);
            Assert.assertEquals(" Type(w)=WROTE}",split[2]);

            Assert.assertEquals(2,countAllNodes());
            Assert.assertEquals(0,countAllRelationships());
        }

        r.close();

        //clean up
        detachDeleteEverything();
    }

    @Test
    public void complexCreateShouldWork() {
        Result r;
        try (Transaction tx = db.beginTx()) {
            r = db.execute("CALL de.saschapeukert.runCypher(\"CREATE (n:Test) " +
                    "WITH n CREATE (m:Person) WITH n,m create " +
                    "(m)-[w:WROTE]->(n) RETURN Labels(n), Labels(m), Type(w)\", null)");
            tx.success();
        }
        Assert.assertNotNull("Result should not be null", r);
        while (r.hasNext()) {
            Map<String, Object> map = r.next();
            Set<String> set = map.keySet();
            Iterator<String> sit = set.iterator();

            String result = map.get(sit.next()).toString();
            String[] split = result.split(",");

            Assert.assertEquals("{Labels(m)=[Person]",split[0]);
            Assert.assertEquals(" Labels(n)=[Test]",split[1]);
            Assert.assertEquals(" Type(w)=WROTE}",split[2]);

            Assert.assertEquals(2,countAllNodes());
            Assert.assertEquals(1,countAllRelationships());
        }
        r.close();

        //clean up
        detachDeleteEverything();
    }

    @Test
    public void complexVirtualCreateQueryShouldWork() {
        Result r;
        try (Transaction tx = db.beginTx()) {
            r = db.execute("CALL de.saschapeukert.runCypher(\"CREATE VIRTUAL (n:Test) " +
                    "WITH n CREATE VIRTUAL (m:Person) WITH n,m create VIRTUAL " +
                    "p = (((m)-[w:WROTE]->(n))) RETURN Labels(n), Labels(m), Type(w)\", null)");
            tx.success();
        }
        Assert.assertNotNull("Result should not be null", r);
        while (r.hasNext()) {
            Map<String, Object> map = r.next();
            Set<String> set = map.keySet();
            Iterator<String> sit = set.iterator();

            String result = map.get(sit.next()).toString();
            String[] split = result.split(",");

            Assert.assertEquals("{Labels(m)=[Person]",split[0]);
            Assert.assertEquals(" Labels(n)=[Test]",split[1]);
            Assert.assertEquals(" Type(w)=WROTE}",split[2]);

            Assert.assertEquals(0,countAllNodes());
            Assert.assertEquals(0,countAllRelationships());
        }
        r.close();
    }


    @Test
    public void queryWithBothCreateStatementsMixedShouldWorkProperly() {
        Result r;
        try (Transaction tx = db.beginTx()) {
            r = db.execute("CALL de.saschapeukert.runCypher(\"CREATE VIRTUAL (n:Test) " +
                    "WITH n CREATE ((m:Person)) WITH n,m create VIRTUAL " +
                    "((m)-[w:WROTE]->(n)) RETURN Labels(n), Labels(m), Type(w)\", null)");
            tx.success();
        }
        Assert.assertNotNull("Result should not be null", r);
        while (r.hasNext()) {
            Map<String, Object> map = r.next();
            Set<String> set = map.keySet();
            Iterator<String> sit = set.iterator();

            String result = map.get(sit.next()).toString();
            String[] split = result.split(",");

            Assert.assertEquals("{Labels(m)=[Person]", split[0]);
            Assert.assertEquals(" Labels(n)=[Test]", split[1]);
            Assert.assertEquals(" Type(w)=WROTE}", split[2]);

            Assert.assertEquals("Only the person node should be persisted",1, countAllNodes());
            Assert.assertEquals("No relationship should exist now",0,countAllRelationships());
        }
        r.close();

        try (Transaction tx = db.beginTx()) {
            r = db.execute("MATCH (n) RETURN Labels(n)");
            Assert.assertNotNull("Result should not be null", r);
            Map<String, Object> map = r.next();
            Set<String> set = map.keySet();
            Iterator<String> sit = set.iterator();

            String result = map.get(sit.next()).toString();
            tx.success();

            Assert.assertEquals("Only the person node should be persisted", "[Person]", result);
        }
        r.close();

        //clean up
        detachDeleteEverything();
    }

    @Test
    public void virtualNodesShouldWork() {
        Result r= createVirtualNode();
        Assert.assertNotNull("Result should not be null", r);
        while (r.hasNext()) {
            Map<String, Object> map = r.next();
            Set<String> set = map.keySet();
            Iterator<String> sit = set.iterator();

            String result = map.get(sit.next()).toString();
            String[] split = result.split(",");
            Assert.assertEquals("Name property should be set to 'Hello'", "Name=Hello", split[0].substring(1));
            Assert.assertEquals("Label of the virtual node should be set to 'TEST'", " Labels=[TEST]", split[1].substring(0, split[1].length() - 1));
        }
        r.close();
    }

    @Test
    public void MergeClauseShouldNotBeAffectedWithoutCreateVirtual(){
        Result r;
        try (Transaction tx = db.beginTx()) {
            r = db.execute("CALL de.saschapeukert.runCypher(\" MERGE (n:Test {Name: 'Hello'}) RETURN " +
                    "n.Name as Name \", null)");
            Assert.assertNotNull("Result should not be null", r);
            tx.success();

            while (r.hasNext()) {
                Map<String, Object> map = r.next();
                Set<String> set = map.keySet();
                Iterator<String> sit = set.iterator();

                String result = map.get(sit.next()).toString();
                Assert.assertEquals("Name property should be set to 'Hello'", "{Name=Hello}", result);
            }
            r.close();
        }
        //clean up
        detachDeleteEverything();
    }

    @Test
    public void ifVirtualEntitiesAreMentionedInMergeTheyShallRemainVirtual(){
        Result r;
        try (Transaction tx = db.beginTx()) {
            r = db.execute("CALL de.saschapeukert.runCypher(\" CREATE VIRTUAL (n:Test {Name: 'Hello'}) " +
                    "MERGE (m:Test {Name: 'Hello'}) RETURN n.Name, id(n) ,m.Name, id(m)" +
                    " \", null)");
            Assert.assertNotNull("Result should not be null", r);
            tx.success();

            // Test if they get the same result
            while (r.hasNext()) {
                Map<String, Object> map = r.next();
                Set<String> set = map.keySet();
                Iterator<String> sit = set.iterator();

                String result = map.get(sit.next()).toString();
                result = result.substring(1,result.length()-1);
                result = result.replace(" ","");
                String[] split = result.split(",");  // seems that the ordering is fixed
                Assert.assertEquals("Name property should be set to 'Hello'", "n.Name=Hello", split[2]);
                Assert.assertEquals("Both entities should be the same, so their ids should match ",
                        split[0].substring(6), split[1].substring(6)); // everything after id(x)=
                Assert.assertEquals("Both entities should be the same, so their properties should match ",
                        split[2].substring(7), split[3].substring(7));  // everything after x.Name=
            }
            r.close();
        }
        // Test if they persisted the virtual node
        Assert.assertEquals("Virtual Node should not be persisted",0,countAllNodes());


        // Test if they persisted a real relationship connected to a virtual node (should not)
        try (Transaction tx = db.beginTx()) {
            r = db.execute("CALL de.saschapeukert.runCypher(\" CREATE VIRTUAL (n:Test {Name: 'Hello'}) " +
                    "MERGE (n)<-[:WROTE]-(m:Person) " +
                    " \", null)");
            Assert.assertNotNull("Result should not be null", r);
            tx.success();
        } catch (Exception e){
            Assert.assertEquals("Transaction was marked as successful, but unable to commit transaction so rolled back.",
                    e.getMessage());
        }
        Assert.assertEquals("No Node should be persisted",0,countAllNodes());
        Assert.assertEquals("No relationship should be persisted",0,countAllRelationships());

        //clean up
        detachDeleteEverything();
    }

    @Test
    public void virtualNodesShouldLookLikePersistedDuringATransaction() {
        Result r;
        try (Transaction tx = db.beginTx()) {
            Result e = db.execute("CALL de.saschapeukert.runCypher(\"CREATE VIRTUAL (n:TEST{name: 'Hello'}) " +
                    "\", null)");
            Assert.assertNotNull("Result should not be null", e);
            r = db.execute("MATCH (n:TEST) RETURN n.name as Name, id(n) as Id");
            tx.success();

            Assert.assertNotNull("Result should not be null", r);
            String id="";
            while (r.hasNext()) {
                Map<String, Object> map = r.next();
                Set<String> set = map.keySet();
                Iterator<String> sit = set.iterator();

                String result = map.get(sit.next()).toString();
                id = map.get(sit.next()).toString();
                Assert.assertNotEquals("Id should not be null", "", id);
                Assert.assertEquals("Name property should be set to 'Hello'", "Hello", result);
            }
            r = db.execute("MATCH (n:TEST) RETURN id(n) as Id");
            tx.success();
            while (r.hasNext()) {
                Map<String, Object> map = r.next();
                Set<String> set = map.keySet();
                Iterator<String> sit = set.iterator();

                String id_new = map.get(sit.next()).toString();
                Assert.assertEquals("Id should be consistant throughout the transaction", id, id_new);
            }
            r.close();
        }
    }

    @Test
    public void realNodesShouldWork() {
        Result r= createRealNode();
        Assert.assertNotNull("Result should not be null", r);
        while (r.hasNext()) {
            Map<String, Object> map = r.next();
            Set<String> set = map.keySet();
            Iterator<String> sit = set.iterator();

            String result = map.get(sit.next()).toString();
            String[] split = result.split(",");
            Assert.assertEquals("Name property should be set to 'Hello'", "Name=Hello", split[0].substring(1));
            Assert.assertEquals("Label of the virtual node should be set to 'TEST'", " Labels=[TEST]",
                    split[1].substring(0, split[1].length() - 1));
        }
        r.close();
        //Clean up
        detachDeleteEverything();
    }

    @Test
    public void realRelationshipsShouldWork() {
        Result r= createRealRelationship();
        Assert.assertNotNull("Result should not be null", r);
        while (r.hasNext()) {
            Map<String, Object> map = r.next();
            Set<String> set = map.keySet();
            Iterator<String> sit = set.iterator();

            String result = map.get(sit.next()).toString();
            result = result.substring(1,result.length()-1);
            result = result.replace(" ","");
            String[] split = result.split(",");
            Assert.assertEquals("Type should be set to 'WROTE'", "Type=WROTE", split[0]);
            Assert.assertEquals("Property of the virtual relationship should be set to 'Timestamp=today'", "Timestamp=today",
                    split[1]);
        }
        r.close();
        //Clean up
        detachDeleteEverything();
    }

    @Test
    public void virtualRelationshipsShouldWork() {
        Result r= createVirtualRelationship();
        Assert.assertNotNull("Result should not be null", r);
        while (r.hasNext()) {
            Map<String, Object> map = r.next();
            Set<String> set = map.keySet();
            Iterator<String> sit = set.iterator();

            String result = map.get(sit.next()).toString();
            result = result.substring(1,result.length()-1);
            result = result.replace(" ","");
            String[] split = result.split(",");
            Assert.assertEquals("Type should be set to 'WROTE'", "Type=WROTE", split[0]);
            Assert.assertEquals("Property of the virtual relationship should be set to 'Timestamp=today'", "Timestamp=today",
                    split[1]);
        }
        r.close();
    }

    @Test
    public void virtualNodesShouldntBePersisted() {
        Result r= createVirtualNode();
        Assert.assertNotNull("Result should not be null", r);
        Assert.assertEquals(0,countAllNodes());
    }

    @Test
    public void virtualRelationshipsShouldntBePersisted() {
        Result r= createVirtualRelationship();
        Assert.assertNotNull("Result should not be null", r);
        Assert.assertEquals(0,countAllNodes());
        Assert.assertEquals(0,countAllRelationships());
    }

    @Test
    public void realNodesShouldBePersisted() {
        Result r= createRealNode();
        Assert.assertNotNull("Result should not be null", r);
        Assert.assertEquals(1,countAllNodes());
        //Clean up
        detachDeleteEverything();
    }

    @Test
    public void realRelationshipsShouldBePersisted(){
        Result r= createRealRelationship();
        Assert.assertNotNull("Result should not be null", r);
        Assert.assertEquals(1,countAllRelationships());
        Assert.assertEquals(2,countAllNodes());
        //Clean up
        detachDeleteEverything();
    }

    private Result createRealNode(){
        Result r;
        try (Transaction tx = db.beginTx()) {
            r = db.execute("CALL de.saschapeukert.runCypher(\"CREATE (n:TEST{name: 'Hello'"+
                    "}) RETURN n.name as Name, Labels(n) as Labels\", null)");
            tx.success();
        }
        return r;
    }

    private Result createRealRelationship(){
        Result r;
        try (Transaction tx = db.beginTx()) {
            r = db.execute("CALL de.saschapeukert.runCypher(\"CREATE (n:TEST{name: 'Hello'"+
                    "})<-[w:WROTE{timestamp:'today'}]-(m:Person) RETURN type(w) AS Type, w.timestamp AS Timestamp" +
                    "\", null)");
            tx.success();
        }
        return r;
    }

    private Result createVirtualRelationship(){
        Result r;
        try (Transaction tx = db.beginTx()) {
            r = db.execute("CALL de.saschapeukert.runCypher(\"CREATE VIRTUAL (n:TEST{name: 'Hello'"+
                    "})<-[w:WROTE{timestamp:'today'}]-(m:Person) RETURN type(w) AS Type, w.timestamp AS Timestamp" +
                    "\", null)");
            tx.success();
        }
        return r;
    }

    private Result createVirtualNode(){
        Result r;
        try (Transaction tx = db.beginTx()) {
            r = db.execute("CALL de.saschapeukert.runCypher(\"CREATE VIRTUAL (n:TEST{name: 'Hello'})" +
                    " RETURN n.name as Name, Labels(n) as Labels\", null)");
            tx.success();
        }
        return r;
    }

    private Result detachDeleteEverything(){
        Result r;
        try (Transaction tx = db.beginTx()) {
            r = db.execute("MATCH (n) DETACH DELETE n");
            tx.success();
        }
        return r;
    }

    public static void registerProcedure(GraphDatabaseService db, Class<?> procedure) throws KernelException {
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).register(procedure);
    }

    private int countAllNodes(){
        Result r;
        try (Transaction tx = db.beginTx()) {
            r = db.execute("MATCH(n) RETURN COUNT(n)");
            tx.success();
        }
        Assert.assertNotNull("Result should not be null",r);
        while(r.hasNext()) {
            Map<String,Object> map = r.next();
            Set<String> set =map.keySet();
            Iterator<String> sit = set.iterator();
            r.close();
            return Integer.parseInt(map.get(sit.next()).toString());
        }
        r.close();
        return -1;
    }

    private int countAllRelationships(){
        Result r;
        try (Transaction tx = db.beginTx()) {
            r = db.execute("MATCH()-[t]->() RETURN COUNT(t)");

            tx.success();
        }
        Assert.assertNotNull("Result should not be null",r);
        while(r.hasNext()) {
            Map<String,Object> map = r.next();
            Set<String> set =map.keySet();
            Iterator<String> sit = set.iterator();
            r.close();
            return Integer.parseInt(map.get(sit.next()).toString());
        }
        r.close();
        return -1;
    }
}
