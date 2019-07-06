package soton.want.calcite.operators.physic;

import org.apache.calcite.rel.SingleRel;
import soton.want.calcite.operators.TupleQueue;

/**
 * @author want
 */
public abstract class UnaryOperator<T extends SingleRel> extends AbstractOperator{
    protected AbstractOperator child;
    protected TupleQueue source;


    public UnaryOperator(T logicalNode, AbstractOperator child) {
        super(logicalNode);
        this.child = child;
        this.source = new TupleQueue();
        this.child.addParent(this);
        this.child.addSink(this.source);
    }

    @Override
    public Operator[] getChildren() {
        return new Operator[]{child};
    }

    @Override
    public void explain(int n) {
        printTab(n);
        System.out.println(logicalNode.getDigest());
        this.child.explain(n+1);
    }

    @Override
    public TupleQueue[] getSources() {
        return new TupleQueue[]{source};
    }
}
