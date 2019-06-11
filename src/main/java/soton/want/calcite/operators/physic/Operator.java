package soton.want.calcite.operators.physic;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import soton.want.calcite.operators.TupleQueue;

/**
 * @author want
 */
public interface Operator {
    Operator[] getChildren();

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
    TupleQueue getSink();


    void setParent(Operator parent);

    /**
     * run parent operator
     */
    void runParent();

}
