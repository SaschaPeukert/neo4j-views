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
import java.util.List;
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

    @Test
    public void extractVirtualPartShouldWorkProperly(){
        BaselineProcedure sut = new BaselineProcedure();

        String count ="MATCH(n) RETURN COUNT(n)";
        String oneVirtual = "CREATE VIRTUAL (n:Test) RETURN n";
        String threeVirtual = "CREATE VIRTUAL (n:Test) CREATE VIRTUAL (m:Person) WITH n,m create VIRTUAL m-[:WROTE]->n RETURN n";
        String oneVirtualoneReal = "CREATE VIRTUAL (n:Test) CREATE (m:Person) RETURN n,m";

        List<PathReplacement> list =sut.extractVirtualPart(count);
        Assert.assertEquals("Querys without CREATE VIRTUAL should not be appear in the list at all",0,list.size());

        list =sut.extractVirtualPart(oneVirtual);
        Assert.assertEquals("Failed to recognize Statement: " + oneVirtual,1,list.size());
        Assert.assertEquals("Failed to extract Statement: " + oneVirtual,"(n:Test)",list.get(0).getOriginalPathString());

        Assert.assertEquals("Wrong Startposition of PathReplacement for (n:Test)",15,list.get(0).getStartPos());
        Assert.assertEquals("Wrong Endposition of PathReplacement for (n:Test)",22,list.get(0).getEndPos());

        list =sut.extractVirtualPart(threeVirtual);
        Assert.assertEquals("Failed to recognize Statement: " + threeVirtual,3,list.size());
        Assert.assertEquals("Failed to extract first path in Statement: " + threeVirtual,"(n:Test)",list.get(0)
                .getOriginalPathString());
        Assert.assertEquals("Failed to extract second path in Statement: " + threeVirtual,"(m:Person)",list.get(1)
                .getOriginalPathString());
        Assert.assertEquals("Failed to extract third path in Statement: " + threeVirtual,"m-[:WROTE]->n",list.get(2)
                .getOriginalPathString());

        Assert.assertEquals("Wrong Startposition of PathReplacement for (n:Test)",15,list.get(0).getStartPos());
        Assert.assertEquals("Wrong Endposition of PathReplacement for (n:Test)",22,list.get(0).getEndPos());
        Assert.assertEquals("Wrong Startposition of PathReplacement for (m:Person)",39,list.get(1).getStartPos());
        Assert.assertEquals("Wrong Endposition of PathReplacement for (m:Person)",48,list.get(1).getEndPos());
        Assert.assertEquals("Wrong Startposition of PathReplacement for m-[:WROTE]->n",74,list.get(2).getStartPos());
        Assert.assertEquals("Wrong Endposition of PathReplacement for m-[:WROTE]->n",86,list.get(2).getEndPos());

        list =sut.extractVirtualPart(oneVirtualoneReal);
        Assert.assertEquals("Failed to recognize Statement: " + oneVirtualoneReal,1,list.size());
        Assert.assertEquals("Failed to extract Statement: " + oneVirtualoneReal,"(n:Test)",list.get(0)
                .getOriginalPathString());
    }

    @Test
    public void removeWhitespaceShouldWorkFine(){
        BaselineProcedure sut = new BaselineProcedure();

        String count ="MATCH (n) RETURN COUNT (n)";
        String blownCount = "MATCH (n)      RETURN    COUNT (n)";

        Assert.assertEquals(count,sut.replaceStringFromStatement(blownCount,"  "," "));
    }

    @Test
    public void replacePathWithVirtualPathsShouldWork(){
        BaselineProcedure sut = new BaselineProcedure();

        String threeVirtual = "CREATE VIRTUAL (n:Test) CREATE VIRTUAL (m:Person) WITH n,m create VIRTUAL m-[:WROTE]->n RETURN n";
        List<PathReplacement> list =sut.extractVirtualPart(threeVirtual);

        String result = sut.replacePathWithVirtualPaths(threeVirtual,list);
        String inject = "{" + BaselineProcedure.PROPERTYKEY + ":true}";
        String correctResult = "CREATE VIRTUAL (n:Test"+inject+") CREATE VIRTUAL (m:Person"+inject+") WITH n,m create VIRTUAL m-[:WROTE"+inject+"]->n RETURN n";

        Assert.assertEquals(correctResult,result);
    }

    @Test
    public void replaceCreateVirtualWithCreateShouldWork(){
        BaselineProcedure sut = new BaselineProcedure();

        String teststring ="CREATE (n:Person) WITH n CREATE VIRTUAL (t:TEST) CREATE VIRTUAL n-[:WROTE]->t";
        String resultString = "CREATE (n:Person) WITH n CREATE (t:TEST) CREATE n-[:WROTE]->t";

        Assert.assertEquals(resultString,sut.replaceStringFromStatement(teststring,"CREATE VIRTUAL ","CREATE "));
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
