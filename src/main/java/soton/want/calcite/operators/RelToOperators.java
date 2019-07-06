package soton.want.calcite.operators;


import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import soton.want.calcite.operators.logic.LogicalDelta;
import soton.want.calcite.operators.logic.LogicalRStream;
import soton.want.calcite.operators.logic.LogicalWindow;
import soton.want.calcite.operators.physic.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class visits Logical plan and generate physical operators
 * @author want
 */
public class RelToOperators {

    /**
     * leaf nodes of the physical plan
     * the start point of the physical operators
     */
    private List<Operator> tables = new ArrayList<>();

    private Map<RelNode,Operator> instancesMap = new HashMap<>();

    public Operator visit(TableScan scan) {
        Operator op = new TableScanOperator(scan);
        tables.add(op);
        return op;
    }

    public  List<Operator> getTables() {
        return tables;
    }

    public Operator visit(LogicalFilter filter) {
        Operator childOp = visit(filter.getInput());
        return new FilterOperator(filter,childOp);
    }


    public Operator visit(LogicalProject project) {
        Operator childOp = visit(project.getInput());
        return new ProjectOperator(project,childOp);
    }

    public Operator visit(LogicalDelta rStream){
        Operator childOp = visit(rStream.getInput());
        return new DeltaOperator(rStream,childOp);
    }

    public Operator visit(LogicalRStream rStream){
        Operator childOp = visit(rStream.getInput());
        return new RStreamOperator(rStream,childOp);
    }


    public Operator visit(LogicalJoin join) {
        Operator left = visit(join.getInput(0));
        Operator right = visit(join.getInput(1));

        return new JoinOperator(join,left,right);
    }

    public Operator visit(LogicalAggregate aggregate) {
        Operator childOp = visit(aggregate.getInput());
        return new GroupByOperator(aggregate,childOp);
    }


    public Operator visit(LogicalUnion union) {
        return null;
    }

    public Operator visit(LogicalSort sort) {
        return null;
    }

    public Operator visit(LogicalWindow tupleWindow){
        Operator childOp = visit(tupleWindow.getInput());
        return new WindowOperator(tupleWindow,childOp);
    }


    public Operator visit(RelNode relNode) {
        if (instancesMap.containsKey(relNode)) {
            return instancesMap.get(relNode);
        }
        Operator operator = null;
        if (relNode instanceof LogicalFilter){
            operator = visit((LogicalFilter) relNode);
        }else if (relNode instanceof LogicalProject){
            operator = visit((LogicalProject) relNode);
        }else if (relNode instanceof LogicalWindow){
            operator =  visit((LogicalWindow) relNode);
        }else if (relNode instanceof TableScan){
            operator =  visit((TableScan) relNode);
        }else if (relNode instanceof LogicalJoin){
            operator =  visit((LogicalJoin) relNode);
        }else if (relNode instanceof LogicalAggregate){
            operator =  visit((LogicalAggregate) relNode);
        }else if (relNode instanceof LogicalRStream) {
            operator = visit((LogicalRStream) relNode);
        }else if (relNode instanceof LogicalDelta) {
            operator = visit((LogicalDelta) relNode);
        }

        instancesMap.put(relNode,operator);

        return operator;
    }
}
