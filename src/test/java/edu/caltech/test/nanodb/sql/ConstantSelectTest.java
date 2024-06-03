package edu.caltech.test.nanodb.sql;

import edu.caltech.nanodb.server.CommandResult;
import org.testng.annotations.Test;


@Test(groups={"sql", "hw2"})
public class ConstantSelectTest extends SqlTestCase {

    public ConstantSelectTest() {
        super("setup_testSimpleSelects");
    }

    public void testSelect1() {
        try {
            CommandResult commandResult = tryDoCommand("select 1+2, 5*21, 3 +(4*5);");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void basicSelectAnd() {

        try {
//            tryDoCommand("select * from test_simple_selects");
            var r = tryDoCommand("select t.a, t.b from test_simple_selects as t where a>1");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void basicGroupBy() {

        try {
//            tryDoCommand("select * from test_simple_selects");
            var r = tryDoCommand("select t.a, t.b, count(t.a), max(t.a) from test_simple_selects as t group by t.a, t.b");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void basicGroupByAndHaving() {

        try {
//            tryDoCommand("select * from test_simple_selects");
            var r = tryDoCommand("select t.a, count(t.a) from test_simple_selects as t where a>1 group by t.a");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
