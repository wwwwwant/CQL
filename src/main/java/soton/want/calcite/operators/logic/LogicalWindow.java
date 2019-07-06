package soton.want.calcite.operators.logic;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * LogicalWindow which transform a Stream to a Relation based on condition (fixed tuple size or time interval)
 * @author want
 */
public class LogicalWindow extends SingleRel {

    private RexNode condition;

    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param input   Input relational expression
     */
    protected LogicalWindow(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexNode condition) {
        super(cluster, traits, input);
        this.condition = condition;
    }

    public static LogicalWindow create(RelNode input, RexNode windowSize){
        RelOptCluster cluster = input.getCluster();
        RelTraitSet traits= input.getTraitSet();
        return new LogicalWindow(cluster,traits,input,windowSize);
    }

    public RexNode getCondition() {
        return condition;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        Object val = ((RexLiteral) condition).getValue2();
        if (condition.getType().getSqlTypeName().equals(SqlTypeName.TIME)){
            return super.explainTerms(pw)
                    .item("TupleTimeWindow", val+" ms");
        }else {
            long l =  (Long) val;
            if (l>0){
                return super.explainTerms(pw)
                        .item("TupleSizeWindow",l);
            }else{
                return super.explainTerms(pw).item("unBoundedWindow","infinite");
            }
        }

    }
}
