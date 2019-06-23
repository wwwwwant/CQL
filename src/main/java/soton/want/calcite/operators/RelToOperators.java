package soton.want.calcite.operators;


import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import soton.want.calcite.operators.logic.LogicalDelta;
import soton.want.calcite.operators.logic.LogicalTupleWindow;
import soton.want.calcite.operators.physic.*;

import java.util.ArrayList;
import java.util.List;

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

    public Operator visit(LogicalTupleWindow tupleWindow){
        Operator childOp = visit(tupleWindow.getInput());
        return new WindowOperator(tupleWindow,childOp);
    }


    public Operator visit(RelNode other) {
        if (other instanceof LogicalFilter){
            return visit((LogicalFilter) other);
        }else if (other instanceof LogicalProject){
            return visit((LogicalProject) other);
        }else if (other instanceof LogicalTupleWindow){
            return visit((LogicalTupleWindow) other);
        }else if (other instanceof TableScan){
            return visit((TableScan) other);
        }else if (other instanceof LogicalJoin){
            return visit((LogicalJoin) other);
        }else if (other instanceof LogicalAggregate){
            return visit((LogicalAggregate) other);
        }
        return null;
    }
}
