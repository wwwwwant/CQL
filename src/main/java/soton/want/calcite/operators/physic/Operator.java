package soton.want.calcite.operators.physic;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import soton.want.calcite.operators.Tuple;
import soton.want.calcite.operators.TupleQueue;

import java.util.List;

/**
 * @author want
 */
public interface Operator {
    Operator[] getChildren();

    TupleQueue[] getSources();

    List<Operator> getParents();

    /**
     * run operator to consume tuple from source and generate tuple to sink
     */
    void run();

    RelNode getLogicalNode();

    /**
     * return the type information of the tuple
     * @return
     */
    RelDataType getRowType();

    /**
     * sink contains the output tuple of the operator
     * @return the sink of the Operator
     */
    List<TupleQueue> getSinks();

    void explain(int n);

}
