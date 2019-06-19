package soton.want.calcite.operators.physic;

import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import org.apache.log4j.Logger;
import soton.want.calcite.operators.Tuple;

/**
 * @author want
 */
public class FilterOperator extends UnaryOperator<Filter> {

    private static final Logger LOGGER = Logger.getLogger(FilterOperator.class);

    private RexNode condition;

    public FilterOperator(Filter logicalNode, Operator child) {
        super(logicalNode, child);
        this.condition = logicalNode.getCondition();
    }

    @Override
    public void doRun() {
        LOGGER.debug("run filter......");

        Tuple tuple = null;
        while ((tuple=source.pollFirst())!=null){
            Boolean comp = (Boolean) Eval.eval(condition,tuple);
            if (comp!=null && comp){
                this.sink.addLast(tuple);
            }
        }
    }
}
