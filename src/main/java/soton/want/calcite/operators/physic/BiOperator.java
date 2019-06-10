package soton.want.calcite.operators.physic;

import org.apache.calcite.rel.BiRel;
import soton.want.calcite.operators.TupleQueue;

/**
 * @author want
 */
public abstract class BiOperator<T extends BiRel> extends AbstractOperator{
    protected Operator left;
    protected Operator right;

    protected TupleQueue leftSource;
    protected TupleQueue rightSource;

    public BiOperator(T logicalNode, Operator left, Operator right) {
        super(logicalNode);
        this.left = left;
        this.right = right;

        this.left.setParent(this);
        this.right.setParent(this);

        leftSource = left.getSink();
        rightSource = right.getSink();
    }

    @Override
    public Operator[] getChildren() {
        return new Operator[]{left,right};
    }
}
