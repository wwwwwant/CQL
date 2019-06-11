package soton.want.calcite.operators.physic;

import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import soton.want.calcite.operators.Tuple;

import java.util.List;

/**
 * @author want
 */
public class ProjectOperator extends UnaryOperator<Project> {

    List<RexNode> projectList;

    public ProjectOperator(Project project, Operator child) {
        super(project, child);
        this.projectList = project.getProjects();
    }

    @Override
    public void run() {

        Tuple tuple;
        Tuple res = null;
        while ((tuple=source.pollFirst())!=null){
            Object[] result = new Object[projectList.size()];

            // get project fields
            for (int i=0;i<projectList.size();i++){
                result[i] = Eval.eval(projectList.get(i),tuple);
            }
            res = new Tuple(result,getRowType(),tuple.getState());
            sink.addLast(res);
        }

        runParent();

    }
}
