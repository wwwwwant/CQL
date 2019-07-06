package soton.want.calcite.operators.physic;

import soton.want.calcite.operators.Tuple;
import soton.want.calcite.operators.TupleQueue;
import soton.want.calcite.operators.Utils;
import soton.want.calcite.operators.logic.LogicalRStream;

import java.util.Iterator;

/**
 * @author want
 */
public class RStreamOperator extends UnaryOperator<LogicalRStream> {
    private TupleQueue synopsis;
    public RStreamOperator(LogicalRStream logicalNode, Operator child) {
        super(logicalNode, (AbstractOperator) child);
        this.synopsis = new TupleQueue();
    }

    @Override
    protected void doRun() {
        Tuple tuple;
        while ((tuple=source.pollFirst())!=null){
            if (tuple.getState().equals(Tuple.State.ADD)){
                this.synopsis.addLast(tuple);
            }else{
                Iterator<Tuple> iterator = synopsis.iterator();
                while (iterator.hasNext()){
                    if (iterator.next().equals(tuple)) {
                        iterator.remove();
                        break;
                    }
                }
            }
        }
        Iterator<Tuple> iterator = synopsis.iterator();

        while (iterator.hasNext()){
            tuple = iterator.next();
            //TODO more elegant way to print result
            if (this.parents.size()==0){
                Utils.printTuple(tuple);
            }else {
                sendToSinks(tuple);
            }
        }
    }
}
