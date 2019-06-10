package soton.want.calcite.operators.physic;

import org.apache.calcite.rel.SingleRel;
import soton.want.calcite.operators.TupleQueue;

/**
 * @author want
 */
public abstract class UnaryOperator<T extends SingleRel> extends AbstractOperator{
    protected Operator child;
    protected TupleQueue source;


    public UnaryOperator(T logicalNode, Operator child) {
        super(logicalNode);
        this.child = child;
        this.source = child.getSink();
        this.child.setParent(this);
    }

    @Override
    public Operator[] getChildren() {
        return new Operator[]{child};
    }


}
