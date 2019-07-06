package soton.want.calcite.operators.logic;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

/**
 * @author want
 */
public class LogicalRStream extends SingleRel {
    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param input   Input relational expression
     */
    protected LogicalRStream(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);
    }

    public static LogicalRStream create(RelNode input) {
        return new LogicalRStream(input.getCluster(),input.getTraitSet(),input);
    }
}
