import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import soton.want.calcite.operators.RelToOperators;
import soton.want.calcite.operators.Tuple;
import soton.want.calcite.operators.logic.LogicalDelta;
import soton.want.calcite.operators.logic.LogicalTupleWindow;
import soton.want.calcite.operators.physic.Operator;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static soton.want.calcite.operators.Utils.getRelBuilder;

/**
 * @author want
 */
public class testCQL {

    /**
     * RelBuilder is used to build logical plan with api and schema
     * no need to parse SQL
     */
    private static RelBuilder builder = getRelBuilder("sales.json");


    public static void main(String[] args) {


        // test join on stream and relation
        streamRelationJoin();

        // test project and filter on stream
//        streamProjectFilter();



    }


    private static void buildAgg(){
        builder.scan("Orders")
                .aggregate(builder.groupKey("USERID"),
                        builder.count(false, "C"),
                        builder.max("MAX",builder.field("ID")),
                        builder.sum(false, "S", builder.field("UNITS")))
                .filter(
                        builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("C"),
                                builder.literal(10)))
                .sort(builder.field("C"));

        RelNode node = builder.build();

        RelOptUtil.toString(node);
    }


    public static void streamRelationJoin(){
        /**
         * SELECT STREAM ID, USERID, USERID0, NAME
         * FROM Orders[ROW 30] as o join User as u
         * ON o.USERID=U.USERID
         */
        RelNode t1 = builder.scan("Orders").build();
        LogicalTupleWindow window1 = LogicalTupleWindow.create(t1, builder.literal(30));

        RelNode t2 = builder.scan("User").build();

        builder.push(window1);
        builder.push(t2);

        builder.join(JoinRelType.INNER,"USERID");

        List<RexNode> fields = new ArrayList<>();
        int[] fieldId = new int[]{1,2,5,6};

        for (int id: fieldId){
            fields.add(builder.field(id));
        }

        builder.project(fields);

        RelNode project = builder.build();

        LogicalDelta rStream = LogicalDelta.create(project,builder.literal("RET"));

        System.out.println(RelOptUtil.toString(rStream));

        RelToOperators visitor = new RelToOperators();

        visitor.visit(rStream);

        List<Operator> tables = visitor.getTables();

        testOperator(tables);
    }

    public static void streamProjectFilter(){

        /**
         * SELECT stream ID, PRODUCT
         * FROM Orders[ROW 50]
         * WHERE ID>3;
         */
        RelNode relScan = builder.scan("Orders").build();
        LogicalTupleWindow relWindow = LogicalTupleWindow.create(relScan, builder.literal(50));
        RelNode relProject = builder
                .push(relWindow)
                .filter(builder.call(SqlStdOperatorTable.GREATER_THAN,builder.field("ID"),builder.literal(3)))
                .project(builder.field("ID"),builder.field("PRODUCT")).build();

        LogicalDelta rStream = LogicalDelta.create(relProject,builder.literal("RET"));

        RelToOperators visitor = new RelToOperators();
        visitor.visit(rStream);

        List<Operator> tables = visitor.getTables();

        testOperator(tables);

    }


    public static void testOperator(List<Operator> tables){
        while (true){
            System.out.println(new Date(System.currentTimeMillis()));
            System.out.println();
            for (Operator table : tables){
                table.run();
            }
            System.out.println("------------------------------------");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }



}
