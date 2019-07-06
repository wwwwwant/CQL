package soton.want.calcite.operators.physic;

import org.apache.calcite.rel.BiRel;
import soton.want.calcite.operators.TupleQueue;

/**
 * @author want
 */
public abstract class BiOperator<T extends BiRel> extends AbstractOperator{
    protected AbstractOperator left;
    protected AbstractOperator right;

    protected TupleQueue leftSource;
    protected TupleQueue rightSource;

    public BiOperator(T logicalNode, AbstractOperator left, AbstractOperator right) {
        super(logicalNode);
        this.left = left;
        this.right = right;

        this.left.addParent(this);
        this.right.addParent(this);

        leftSource = new TupleQueue();
        rightSource = new TupleQueue();

        this.left.addSink(leftSource);
        this.right.addSink(rightSource);
    }

    @Override
    public Operator[] getChildren() {
        return new Operator[]{left,right};
    }

    @Override
    public void explain(int n) {
        printTab(n);
        System.out.println(logicalNode.getDigest());
        this.left.explain(n+1);
        this.right.explain(n+1);
    }

    @Override
    public TupleQueue[] getSources() {
        return new TupleQueue[]{leftSource,rightSource};
    }
}
