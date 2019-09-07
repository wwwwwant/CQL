package soton.want.calcite.operators.physic;


import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.log4j.Logger;
import soton.want.calcite.operators.Tuple;
import soton.want.calcite.operators.TupleQueue;
import soton.want.calcite.operators.Utils;
import soton.want.calcite.operators.logic.LogicalWindow;

/**
 * Physical Operator {@link LogicalWindow}
 * use synopsis to maintain state
 * @author want
 */
public class WindowOperator extends UnaryOperator<LogicalWindow> {

    private static final Logger LOGGER = Logger.getLogger(WindowOperator.class);

    private long windowSize;
    private int timeInterval;
    private RexNode condition;
    private TupleQueue synopsis;
    private SqlTypeName windowType;

    public WindowOperator(LogicalWindow logicalNode, Operator child) {
        super(logicalNode, (AbstractOperator) child);
        this.condition = logicalNode.getCondition();
        this.windowType = condition.getType().getSqlTypeName();

        if (windowType.equals(SqlTypeName.TIME)){
            this.timeInterval = (Integer) Eval.eval(condition);
        }else{
            this.windowSize =  (Long) Eval.eval(condition);
        }

        this.synopsis = new TupleQueue();
    }

    @Override
    public void doRun() {
        LOGGER.debug("run windowOp......"+logicalNode);
        if (windowType.equals(SqlTypeName.TIME)){
            runTimeWin();
        }else if (windowSize>0){
            runSizeWin();
        }else {
            runUnbounded();
        }
    }

    private void runUnbounded(){
        Tuple tuple = null;
        while ((tuple = source.pollFirst())!=null){
            sendToSinks(new Tuple(tuple, Tuple.State.ADD));
        }
    }

    private void runSizeWin(){
        Tuple tuple;
        while ((tuple=source.pollFirst())!=null){
            if (synopsis.size()==windowSize){
                Tuple delTuple = synopsis.pollFirst();
                sendToSinks(new Tuple(delTuple, Tuple.State.DEL));
            }
            synopsis.addLast(tuple);
            sendToSinks(new Tuple(tuple, Tuple.State.ADD));
        }
    }

    private void runTimeWin(){

        Tuple tuple;
        long startTs = context.getCurrentTs()-timeInterval;
        if (timeInterval==context.getScheduleInterval()) {
            LOGGER.info("windowStartTs: "+Utils.formatDate(startTs));
            LOGGER.info("windowEndTs: "+Utils.formatDate(context.getCurrentTs()));
        }


        // delete tuple out of window
        while (!synopsis.isEmpty()){
            tuple = synopsis.get(0);
            if (tuple.getTs()<startTs){
                Tuple delTuple = synopsis.pollFirst();
                sendToSinks(new Tuple(delTuple,Tuple.State.DEL));
            }else break;
        }

        // add new tuple to sink
        while ((tuple = source.pollFirst())!=null){
            if (tuple.getTs()>startTs || timeInterval == 0){
                synopsis.addLast(tuple);
                sendToSinks(new Tuple(tuple, Tuple.State.ADD));
            }
        }
    }


}
