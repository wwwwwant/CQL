package soton.want.calcite.operators.physic;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import soton.want.calcite.operators.Context;
import soton.want.calcite.operators.Tuple;
import soton.want.calcite.operators.TupleQueue;

import java.util.ArrayList;
import java.util.List;

/**
 * @author want
 */
public abstract class AbstractOperator<T extends RelNode> implements Operator{
    T logicalNode;
    protected List<TupleQueue> sinks;
    protected List<Operator> parents;
    protected Context context = Context.getInstance();

    public AbstractOperator(T logicalNode) {
        this.logicalNode = logicalNode;
        this.sinks = new ArrayList<>();
        this.parents = new ArrayList<>();
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
    public List<TupleQueue> getSinks() {
        return ImmutableList.copyOf(this.sinks);
    }

    protected void addParent(Operator parent) {
        this.parents.add(parent);
    }

    protected void addSink(TupleQueue sink) {
        this.sinks.add(sink);
    }

    protected abstract void doRun();

    protected void sendToSinks(Tuple tuple){
        for (TupleQueue sink : sinks){
            sink.addLast(tuple);
        }
    }

    @Override
    public void explain(int n) {
        printTab(n);
        System.out.println(logicalNode.getDigest());
    }

    protected void printTab(int n){
        for (int i =0;i<n;i++){
            System.out.print("| ");
        }
    }

    @Override
    public List<Operator> getParents() {
        return parents;
    }

    @Override
    public void run() {
        doRun();
//        runParent();
    }

//    private void runParent(){
//        if (parents.size()!=0 && sinks.get(0).size()>0){
//            for (Operator parent: parents){
//                parent.run();
//            }
//        }
//    }
}
