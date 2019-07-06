package soton.want.calcite.operators.physic;

import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.log4j.Logger;
import soton.want.calcite.operators.Tuple;

import java.util.List;

/**
 * @author want
 */
public class ProjectOperator extends UnaryOperator<Project> {

    private static final Logger LOGGER = Logger.getLogger(ProjectOperator.class);

    List<RexNode> projectList;

    public ProjectOperator(Project project, Operator child) {
        super(project, (AbstractOperator) child);
        this.projectList = project.getProjects();
    }

    @Override
    public void doRun() {

        LOGGER.debug("run project......"+logicalNode);

        Tuple tuple;
        Tuple res = null;
        while ((tuple=source.pollFirst())!=null){
            Object[] result = new Object[projectList.size()];

            // get project fields
            for (int i=0;i<projectList.size();i++){
                result[i] = Eval.eval(projectList.get(i),tuple);
            }
            res = new Tuple(result,getRowType(),tuple.getState(),tuple.getTs());
            sendToSinks(res);
        }

    }
}
