package soton.want.calcite.operators.physic;


import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.log4j.Logger;
import soton.want.calcite.operators.Tuple;
import soton.want.calcite.operators.TupleQueue;
import soton.want.calcite.operators.Utils;
import soton.want.calcite.operators.logic.LogicalTupleWindow;

import java.math.BigDecimal;
import java.util.Date;

/**
 * Physical Operator {@link LogicalTupleWindow}
 * use synopsis to maintain state
 * @author want
 */
public class WindowOperator extends UnaryOperator<LogicalTupleWindow> {

    private static final Logger LOGGER = Logger.getLogger(WindowOperator.class);

    private long windowSize;
    private int timeInterval;
    private RexNode condition;
    private TupleQueue synopsis;

    public WindowOperator(LogicalTupleWindow logicalNode, Operator child) {
        super(logicalNode, child);
        this.condition = logicalNode.getCondition();

        if (condition.getType().getSqlTypeName().equals(SqlTypeName.TIME)){
            this.timeInterval = (Integer) Eval.eval(condition);
            this.windowSize = -1;
        }else{
            this.windowSize = ((BigDecimal) Eval.eval(condition)).longValue();
            this.timeInterval = -1;
        }

        this.synopsis = new TupleQueue();
    }

    @Override
    public void doRun() {
        LOGGER.debug("run windowOp......");
        if (windowSize!=-1){
            runSizeWin();
        }else {
            runTimeWin();
        }
    }

    private void runSizeWin(){
        Tuple tuple;
        while ((tuple=source.pollFirst())!=null){
            if (synopsis.size()==windowSize){
                Tuple delTuple = synopsis.pollFirst();
                this.sink.addLast(new Tuple(delTuple, Tuple.State.DEL));
            }
            synopsis.addLast(tuple);
            this.sink.addLast(new Tuple(tuple, Tuple.State.ADD));
        }
    }

    private void runTimeWin(){
        Tuple tuple;
        long endTs = System.currentTimeMillis()-timeInterval;
        LOGGER.info("windowEndTs: "+Utils.formatDate(endTs));


        // delete tuple out of window
        while (!synopsis.isEmpty()){
            tuple = synopsis.get(0);
            if (tuple.getTs()<endTs){
                Tuple delTuple = synopsis.pollFirst();
                sink.addLast(new Tuple(delTuple,Tuple.State.DEL));
            }else break;
        }

        // add new tuple to sink
        while ((tuple = source.pollFirst())!=null){
            LOGGER.info("receive source tupleTs: "+ Utils.formatDate(tuple.getTs()));
            if (tuple.getTs()>endTs){
                synopsis.addLast(tuple);
                sink.addLast(new Tuple(tuple, Tuple.State.ADD));
            }
        }
    }


}
