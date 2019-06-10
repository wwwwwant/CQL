package soton.want.calcite.operators.physic;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import soton.want.calcite.operators.TupleQueue;

/**
 * @author want
 */
public interface Operator {
    Operator[] getChildren();

    void run();

    RelNode getLogicalNode();

    RelDataType getRowType();

    TupleQueue getSink();

    void setParent(Operator parent);

}
