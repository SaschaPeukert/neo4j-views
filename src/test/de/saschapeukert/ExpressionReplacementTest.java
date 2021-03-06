package de.saschapeukert;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by Sascha Peukert on 23.06.2016.
 */
public class ExpressionReplacementTest {

    private String testNode1 = "(he:Person{name:'Bob'})";
    private String testNode2 = "(:Person{name:'Bob'})";
    private String testNode3 = "(:Person)";
    private String testNode4 = "(he:Person)";

    private String testRel1 = "-[:WROTE]->";
    private String testRel2 = "-[:WROTE]-";
    private String testRel3 = "<-[:WROTE]-";
    private String testRel4 = "-[wr:WROTE]->";
    private String testRel5 = "-[wr:WROTE{test:true}]->";
    private String testRel6 = "-[:WROTE{test:true}]->";

    private String testBoth1 = "((:Person{name:'Bob'})-[wr:WROTE{test:true}]->(:Test))";
    private String testBoth2 = "(:Person{name:'Bob'})-[wr:WROTE{test:true}]->(:Test)<-[:WROTE]-(he:Person)";
    private String testBoth3 = "(n)-[:TESTED]->(:Person)";

    private String testComma = "(n:Test), (m:Person)";

    private static ExpressionReplacement SuT;

    @BeforeClass
    public static void setUp() throws Exception {
    }

    @AfterClass
    public static void tearDown() {}

    @Test
    public void injectVirtualPropertiesShouldWorkProperlyOnNodes(){
        SuT = new ExpressionReplacement("",0); // do not care here
        Assert.assertEquals("(he:Person{name:'Bob',"+ BaselineProcedure.PROPERTYKEY+":true})"
                ,SuT.injectVirtualProperties(testNode1,true));
        Assert.assertEquals("(:Person{name:'Bob',"+ BaselineProcedure.PROPERTYKEY+":true})"
                ,SuT.injectVirtualProperties(testNode2,true));
        Assert.assertEquals("(:Person{"+ BaselineProcedure.PROPERTYKEY+":true})"
                ,SuT.injectVirtualProperties(testNode3,true));
        Assert.assertEquals("(he:Person{"+ BaselineProcedure.PROPERTYKEY+":true})"
                ,SuT.injectVirtualProperties(testNode4,true));

    }

    @Test
    public void injectVirtualPropertiesShouldWorkProperlyOnRelationships(){
        SuT = new ExpressionReplacement("",0); // do not care here
        Assert.assertEquals("-[:WROTE{"+BaselineProcedure.PROPERTYKEY+":true}]->"
                ,SuT.injectVirtualProperties(testRel1,false));
        Assert.assertEquals("-[:WROTE{"+BaselineProcedure.PROPERTYKEY+":true}]-"
                ,SuT.injectVirtualProperties(testRel2,false));
        Assert.assertEquals("<-[:WROTE{"+BaselineProcedure.PROPERTYKEY+":true}]-"
                ,SuT.injectVirtualProperties(testRel3,false));
        Assert.assertEquals("-[wr:WROTE{"+BaselineProcedure.PROPERTYKEY+":true}]->"
                ,SuT.injectVirtualProperties(testRel4,false));
        Assert.assertEquals("-[wr:WROTE{test:true,"+BaselineProcedure.PROPERTYKEY+":true}]->"
                ,SuT.injectVirtualProperties(testRel5,false));
        Assert.assertEquals("-[:WROTE{test:true,"+BaselineProcedure.PROPERTYKEY+":true}]->"
                ,SuT.injectVirtualProperties(testRel6,false));
    }

    @Test
    public void enhancePathShouldWorkProperly(){
        SuT = new ExpressionReplacement(testNode1,0);
        Assert.assertEquals(
                "(he:Person{name:'Bob',"+ BaselineProcedure.PROPERTYKEY+":true})"
                ,SuT.getNewExpression()
        );

        SuT = new ExpressionReplacement(testRel5,0);
        Assert.assertEquals(
                "-[wr:WROTE{test:true,"+BaselineProcedure.PROPERTYKEY+":true}]->"
                ,SuT.getNewExpression()
        );

        SuT = new ExpressionReplacement(testBoth1,0);
        Assert.assertEquals(
                "((:Person{name:'Bob',"+BaselineProcedure.PROPERTYKEY+":true})-[wr:WROTE{test:true," +
                BaselineProcedure.PROPERTYKEY+":true}]->(:Test{"+BaselineProcedure.PROPERTYKEY+":true}))"
                ,SuT.getNewExpression()
        );

        SuT = new ExpressionReplacement(testBoth2,0);
        Assert.assertEquals(
                "(:Person{name:'Bob',"+BaselineProcedure.PROPERTYKEY+":true})-[wr:WROTE{test:true," +
                        BaselineProcedure.PROPERTYKEY+":true}]->(:Test{"+BaselineProcedure.PROPERTYKEY
                        +":true})<-[:WROTE{"+BaselineProcedure.PROPERTYKEY+":true}]-(he:Person{"
                        +BaselineProcedure.PROPERTYKEY+":true})"
                ,SuT.getNewExpression()
        );

        SuT = new ExpressionReplacement(testBoth3,0);
        Assert.assertEquals(
                "(n)-[:TESTED{"+BaselineProcedure.PROPERTYKEY+":true}]->(:Person{" +
                        BaselineProcedure.PROPERTYKEY+":true})"
                ,SuT.getNewExpression()
        );

        SuT = new ExpressionReplacement(testComma,0);
        Assert.assertEquals(
                "(n:Test{" + BaselineProcedure.PROPERTYKEY+":true}), (m:Person{" +
                        BaselineProcedure.PROPERTYKEY+":true})"
                ,SuT.getNewExpression()
        );

    }
}
