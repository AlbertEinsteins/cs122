package edu.caltech.nanodb.queryeval;


import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.ExpressionProcessor;
import edu.caltech.nanodb.expressions.FunctionCall;
import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.functions.Function;
import edu.caltech.nanodb.functions.SimpleFunction;
import edu.caltech.nanodb.plannodes.*;
import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.queryast.SelectValue;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.storage.StorageManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * This class generates execution plans for very simple SQL
 * <tt>SELECT * FROM tbl [WHERE P]</tt> queries.  The primary responsibility
 * is to generate plans for SQL <tt>SELECT</tt> statements, but
 * <tt>UPDATE</tt> and <tt>DELETE</tt> expressions will also use this class
 * to generate simple plans to identify the tuples to update or delete.
 */
public class SimplePlanner implements Planner {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = LogManager.getLogger(SimplePlanner.class);


    /** The storage manager used during query planning. */
    protected StorageManager storageManager;


    /** Sets the server to be used during query planning. */
    public void setStorageManager(StorageManager storageManager) {
        if (storageManager == null)
            throw new IllegalArgumentException("storageManager cannot be null");

        this.storageManager = storageManager;
    }


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     */
    @Override
    public PlanNode makePlan(SelectClause selClause,
                             List<SelectClause> enclosingSelects) {

        // For HW1, we have a very simple implementation that defers to
        // makeSimpleSelect() to handle simple SELECT queries with one table,
        // and an optional WHERE clause.
        PlanNode plannode = null;

        if (enclosingSelects != null && !enclosingSelects.isEmpty()) {
            throw new UnsupportedOperationException(
                "Not implemented:  enclosing queries");
        }

        FromClause fromClause = selClause.getFromClause();
        if (fromClause != null) {
            if (!fromClause.isBaseTable()) {
                throw new UnsupportedOperationException(
                        "Not implemented:  joins or subqueries in FROM clause");
            }

            // set simple table scan
            TableInfo tableInfo = storageManager.getTableManager().openTable(fromClause.getTableName());
            plannode = new FileScanNode(tableInfo, null);

            if (fromClause.isRenamed()) {
                plannode = new RenameNode(plannode, fromClause.getResultName());
            }
        } else {  //means it's a constant select
            plannode = new ProjectNode(selClause.getSelectValues());
        }


        // add where
        Expression whereExpr = selClause.getWhereExpr();
        if (whereExpr != null) {
            plannode = new SimpleFilterNode(plannode, whereExpr);
        }

        // group by or/and having
        if (selClause.getHavingExpr() != null || !selClause.getGroupByExprs().isEmpty() || hasAggClause(selClause)) {
            plannode = planAgg(selClause, plannode);
        }

        // order by clause


        plannode.prepare();
        return plannode;
    }


    private boolean hasAggClause(SelectClause selectClause) {
        boolean hasAgg = false;
        for (SelectValue sv : selectClause.getSelectValues()) {
            Expression expr = sv.getExpression();
            if (expr instanceof FunctionCall) {
                hasAgg = true;
                break;
            }
        }
        return hasAgg;
    }


    public PlanNode planAgg(SelectClause selClause, PlanNode child) {
        List<Expression> groupByExpr = selClause.getGroupByExprs();
        Map<String, FunctionCall> aggs = new LinkedHashMap<>();

        for (SelectValue sv : selClause.getSelectValues()) {
            if (!sv.isExpression()) {
                continue;
            }

            Expression expr = sv.getExpression();
            if (expr instanceof FunctionCall) {
                FunctionCall fc = (FunctionCall) expr;
                Function f = fc.getFunction();
                if (f instanceof AggregateFunction) {
                    aggs.put(fc.getColumnInfo(selClause.getFromSchema()).getName(), fc);
                }
            }
        }

        return new HashedGroupAggregateNode(child, groupByExpr, aggs, selClause.getSelectValues());
    }


    /**
     * Constructs a simple select plan that reads directly from a table, with
     * an optional predicate for selecting rows.
     * <p>
     * While this method can be used for building up larger <tt>SELECT</tt>
     * queries, the returned plan is also suitable for use in <tt>UPDATE</tt>
     * and <tt>DELETE</tt> command evaluation.  In these cases, the plan must
     * only generate tuples of type {@link edu.caltech.nanodb.storage.PageTuple},
     * so that the command can modify or delete the actual tuple in the file's
     * page data.
     *
     * @param tableName The name of the table that is being selected from.
     *
     * @param predicate An optional selection predicate, or {@code null} if
     *        no filtering is desired.
     *
     * @return A new plan-node for evaluating the select operation.
     */
    public SelectNode makeSimpleSelect(String tableName, Expression predicate,
                                       List<SelectClause> enclosingSelects) {
        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        if (enclosingSelects != null) {
            // If there are enclosing selects, this subquery's predicate may
            // reference an outer query's value, but we don't detect that here.
            // Therefore we will probably fail with an unrecognized column
            // reference.
            logger.warn("Currently we are not clever enough to detect " +
                "correlated subqueries, so expect things are about to break...");
        }

        // Open the table.
        TableInfo tableInfo =
            storageManager.getTableManager().openTable(tableName);

        // Make a SelectNode to read rows from the table, with the specified
        // predicate.
        SelectNode selectNode = new FileScanNode(tableInfo, predicate);
        selectNode.prepare();
        return selectNode;
    }
}

