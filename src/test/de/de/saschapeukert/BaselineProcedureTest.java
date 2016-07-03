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
    public void complexCreateShouldWork() {
        Result r;
        try (Transaction tx = db.beginTx()) {
            r = db.execute("CALL de.saschapeukert.runCypher(\"CREATE (n:Test) " +
                    "WITH n CREATE (m:Person) WITH n,m create " +
                    "(m)-[w:WROTE]->(n) RETURN Labels(n), Labels(m), Type(w)\", null) yield value");
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
    }

    @Test
    public void complexQueryShouldWork() {
        Result r;
        try (Transaction tx = db.beginTx()) {
            r = db.execute("CALL de.saschapeukert.runCypher(\"CREATE VIRTUAL (n:Test) " +
                    "WITH n CREATE VIRTUAL (m:Person) WITH n,m create VIRTUAL " +
                    "p = (((m)-[w:WROTE]->(n))) RETURN Labels(n), Labels(m), Type(w)\", null) yield value");
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
                    "WITH n CREATE (m:Person) WITH n,m create VIRTUAL " +
                    "(m)-[w:WROTE]->(n) RETURN Labels(n), Labels(m), Type(w)\", null) yield value");
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
            Assert.assertEquals(0,countAllRelationships());
        }
        r.close();

        Transaction tx = db.beginTx();
        r = db.execute("MATCH (n) RETURN Labels(n)");
        Assert.assertNotNull("Result should not be null", r);
        Map<String, Object> map = r.next();
        Set<String> set = map.keySet();
        Iterator<String> sit = set.iterator();

        String result = map.get(sit.next()).toString();
        tx.success();
        tx.close();
        Assert.assertEquals("Only the person node should be persisted","[Person]",result);
        r.close();
    }

    @Test
    public void virtualNodesShouldBeCreatable() {
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
    public void realNodesShouldBeCreatable() {
        Result r= createRealNode();
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
        //Clean up
        detachDeleteAllNodes();
    }

    @Test
    public void virtualNodesShouldntBePersistent() {
        Result r= createVirtualNode();
        Assert.assertNotNull("Result should not be null", r);
        Assert.assertEquals(0,countAllNodes());
    }

    @Test
    public void realNodesShouldntBePersistent() {
        Result r= createRealNode();
        Assert.assertNotNull("Result should not be null", r);
        Assert.assertEquals(1,countAllNodes());
        //Clean up
        detachDeleteAllNodes();
    }

    private Result createRealNode(){
        Result r;
        try (Transaction tx = db.beginTx()) {
            r = db.execute("CALL de.saschapeukert.runCypher(\"CREATE (n:TEST{name: 'Hello'"+
                    "}) RETURN n.name as Name, Labels(n) as Labels\", null) yield value");
            tx.success();
        }
        return r;
    }

    private Result createVirtualNode(){
        Result r;
        try (Transaction tx = db.beginTx()) {
            r = db.execute("CALL de.saschapeukert.runCypher(\"CREATE (n:TEST{name: 'Hello', "+BaselineProcedure.PROPERTYKEY+
                    ": true}) RETURN n.name as Name, Labels(n) as Labels\", null) yield value");
            tx.success();
        }
        return r;
    }

    private Result detachDeleteAllNodes(){
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
