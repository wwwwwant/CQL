package soton.want.calcite.operators.physic;


import soton.want.calcite.operators.Tuple;
import soton.want.calcite.operators.TupleQueue;
import soton.want.calcite.operators.logic.LogicalTupleWindow;

import java.math.BigDecimal;

/**
 * @author want
 */
public class WindowOperator extends UnaryOperator<LogicalTupleWindow> {

    private long windowSize;
    private TupleQueue synopsis;

    public WindowOperator(LogicalTupleWindow logicalNode, Operator child) {
        super(logicalNode, child);
        this.windowSize = ((BigDecimal) Eval.eval(logicalNode.getWindowSize(),null)).longValue();
        this.synopsis = new TupleQueue();
    }

    @Override
    public void run() {

        Tuple tuple;
        while ((tuple=source.pollFirst())!=null){
            if (synopsis.size()==windowSize){
                Tuple delTuple = synopsis.pollFirst();
                this.sink.addLast(new Tuple(delTuple, Tuple.State.DEL));
            }
            synopsis.addLast(tuple);
            this.sink.addLast(new Tuple(tuple, Tuple.State.ADD));
        }

        runParent();

    }


}
