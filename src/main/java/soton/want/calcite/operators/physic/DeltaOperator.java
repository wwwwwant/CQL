package soton.want.calcite.operators.physic;

import org.apache.calcite.rex.RexNode;
import org.apache.log4j.Logger;
import soton.want.calcite.operators.Tuple;
import soton.want.calcite.operators.Utils;
import soton.want.calcite.operators.logic.LogicalDelta;

/**
 * represent 3 kinds of stream
 * RStream, DStream, IStream
 * based on the condition
 * @author want
 */
public class DeltaOperator extends UnaryOperator<LogicalDelta> {

    private static final Logger LOGGER = Logger.getLogger(DeltaOperator.class);

    private RexNode condition;
    private String type;

    public DeltaOperator(LogicalDelta logicalNode, Operator child) {
        super(logicalNode, child);
        this.condition = logicalNode.getCondition();
        this.type = (String)Eval.eval(condition,null);
    }

    @Override
    protected void doRun() {
        LOGGER.debug("run delta......");

        Tuple tuple = source.pollFirst();
        if (tuple == null){
            return;
        }
//        Utils.printTypes(tuple);

        while (tuple!=null){
            if (type.equals(tuple.getState().toString()) || type.equals("RET")){
                Utils.printTuple(tuple);
            }
            tuple = source.pollFirst();
        }
    }

    @Override
    public void run() {
        doRun();
    }
}
