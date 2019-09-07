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
        super(logicalNode, (AbstractOperator) child);
        this.condition = logicalNode.getCondition();
        this.type = (String)Eval.eval(condition,null);
    }

    @Override
    protected void doRun() {
        LOGGER.debug("run delta......"+logicalNode);

        Tuple tuple = source.pollFirst();
        if (tuple!=null && this.parents.size()==0){
            System.out.println(tuple.getRowType().toString());
        }

        while (tuple!=null){
            if (type.equals(tuple.getState().toString())){
                if (this.parents.size() == 0){
                    Utils.printTuple(tuple);
                }else {
                    sendToSinks(tuple);
                }
            }
            tuple = source.pollFirst();
        }
    }

    @Override
    public String toString() {
        return "DeltaOperator:"+type;
    }
}
