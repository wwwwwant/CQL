import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.log4j.Logger;
import soton.want.calcite.operators.Context;
import soton.want.calcite.operators.RelToOperators;
import soton.want.calcite.operators.Utils;
import soton.want.calcite.operators.logic.LogicalDelta;
import soton.want.calcite.operators.logic.LogicalRStream;
import soton.want.calcite.operators.logic.LogicalWindow;
import soton.want.calcite.operators.physic.Operator;

import java.util.ArrayList;
import java.util.List;

import static soton.want.calcite.operators.Utils.getRelBuilder;

/**
 * @author want
 */
public class TestCQL {
    /**
     * RelBuilder is used to build logical plan with api and schema
     * no need to parse SQL
     */
    private static final Logger LOGGER = Logger.getLogger(TestCQL.class);
    private static RelBuilder builder = getRelBuilder("schema.json");
    private static Context context = Context.getInstance();


    public static void main(String[] args) {


        // test join on stream and relation
//        streamRelationJoin();

        // test project and filter on stream
//        streamProjectFilter();

        // test timeWin
        timeWinProject();

        // test timeWinJoin
//        timeWinJoin();

        // agg
//        testAgg();

    }


    private static void timeWinJoin(){
        /**
         * SELECT STREAM ID, USERID, USERID0, NAME
         * FROM Orders[Range 00:00:30] as o join User as u
         * ON o.USERID=U.USERID
         */
        RelNode t1 = builder.scan("Orders").build();
        RexNode timeInterval = Utils.createTimeInterval(builder, 0, 0, 30);
        LogicalWindow window1 = LogicalWindow.create(t1, timeInterval);

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

//        System.out.println(RelOptUtil.toString(rStream));

        RelToOperators visitor = new RelToOperators();

        visitor.visit(rStream);

    }

    private static void timeWinProject(){
        /**
         * SELECT stream ID, PRODUCT
         * FROM Orders[Range 00:00:30]
         * WHERE ID>3;
         */
        RelBuilder builder = getRelBuilder("schema.json");
        RelNode relScan = builder.scan("Orders").build();
        RexNode timeInterval = Utils.createTimeInterval(builder, 0, 0, 30);

        LogicalWindow relWindow = LogicalWindow.create(relScan, timeInterval);
        RelNode relProject = builder
                .push(relWindow)
                .filter(builder.call(SqlStdOperatorTable.GREATER_THAN
                        ,builder.field("ID")
                        ,builder.literal(3)))
                .project(builder.field("ID")
                        ,builder.field("PRODUCT")).build();

        LogicalRStream rStream = LogicalRStream.create(relProject);

        System.out.println(RelOptUtil.toString(rStream));

        RelToOperators visitor = new RelToOperators();
        visitor.visit(rStream);

        List<Operator> tables = visitor.getTables();
        context.runOperator(tables);
    }


    private static void testAgg(){
        RelNode t1 = builder.scan("Orders").build();
        RexNode timeInterval = Utils.createTimeInterval(builder, 0, 0, 30);
        LogicalWindow window1 = LogicalWindow.create(t1, timeInterval);

        builder.push(window1)
                .aggregate(builder.groupKey("USERID","PRODUCT"),
                        builder.count(false, "C"),
                        builder.sum(false, "S", builder.field("UNITS")),
                        builder.avg(false,"AVG",builder.field("UNITS")),
                        builder.max("MAX",builder.field("UNITS")),
                        builder.min("MIN",builder.field("UNITS"))
                        );
//                .filter(
//                        builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("C"),
//                                builder.literal(10)))
//                .sort(builder.field("C"));

        RelNode node = builder.build();

        LogicalDelta rStream = LogicalDelta.create(node,builder.literal("RET"));

        System.out.println(RelOptUtil.toString(rStream));

        RelToOperators visitor = new RelToOperators();
        visitor.visit(rStream);



    }


    public static void streamRelationJoin(){
        /**
         * SELECT STREAM ID, USERID, USERID0, NAME
         * FROM Orders[ROW 30] as o join User as u
         * ON o.USERID=U.USERID
         */
        RelNode t1 = builder.scan("Orders").build();
        LogicalWindow window1 = LogicalWindow.create(t1, builder.literal(30));

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

    }

    public static void streamProjectFilter(){

        /**
         * SELECT stream ID, PRODUCT
         * FROM Orders[ROW 50]
         * WHERE ID>3;
         */
        RelNode relScan = builder.scan("Orders").build();
        LogicalWindow relWindow = LogicalWindow.create(relScan, builder.literal(50));
        RelNode relProject = builder
                .push(relWindow)
                .filter(builder.call(SqlStdOperatorTable.GREATER_THAN,builder.field("ID"),builder.literal(3)))
                .project(builder.field("ID"),builder.field("PRODUCT")).build();

        LogicalDelta rStream = LogicalDelta.create(relProject,builder.literal("RET"));

        RelToOperators visitor = new RelToOperators();
        visitor.visit(rStream);


    }


    public static void testOperator(List<Operator> tables){
        while (true){
            long startTs = System.currentTimeMillis();
            context.setCurrentTs(startTs);
            LOGGER.info("windowEndTs: "+Utils.formatDate(startTs));
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
