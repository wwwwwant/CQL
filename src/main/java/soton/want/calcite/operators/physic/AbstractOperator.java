package soton.want.calcite.operators.physic;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import soton.want.calcite.operators.TupleQueue;

/**
 * @author want
 */
public abstract class AbstractOperator<T extends RelNode> implements Operator{
    T logicalNode;
    protected TupleQueue sink;
    protected Operator parent;

    public AbstractOperator(T logicalNode) {
        this.logicalNode = logicalNode;
        this.sink = new TupleQueue();
    }

    @Override
    public RelNode getLogicalNode() {
        return logicalNode;
    }

    @Override
    public RelDataType getRowType() {
        return logicalNode.getRowType();
    }

    @Override
    public TupleQueue getSink() {
        return this.sink;
    }

    @Override
    public void setParent(Operator parent) {
        this.parent = parent;
    }

    protected abstract void doRun();

    @Override
    public void run() {
        doRun();
        runParent();
    }

    @Override
    public void runParent(){
        if (parent!=null && !this.sink.isEmpty()){
            this.parent.run();
        }
    }
}
