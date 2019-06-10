package soton.want.calcite.plan;

import com.google.common.io.Resources;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;
import soton.want.calcite.operators.logic.LogicalDelta;
import soton.want.calcite.operators.logic.LogicalTupleWindow;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * @author want
 */
public class SimpleQueryPlanner {

    private final Planner planner;

    public static FrameworkConfig getConfig(SchemaPlus schema) {
        final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

        traitDefs.add(ConventionTraitDef.INSTANCE);
        traitDefs.add(RelCollationTraitDef.INSTANCE);

        FrameworkConfig calciteFrameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.configBuilder()
                        // Lexical configuration defines how identifiers are quoted, whether they are converted to upper or lower
                        // case when they are read, and whether identifiers are matched case-sensitively.
                        .setLex(Lex.MYSQL)
                        .build())
                // Sets the schema to use by the planner
                .defaultSchema(schema)
                .traitDefs(traitDefs)
                // Context provides a way to store data within the planner session that can be accessed in planner rules.
                .context(Contexts.EMPTY_CONTEXT)
                // Rule sets to use in transformation phases. Each transformation phase can use a different set of rules.
                .ruleSets(RuleSets.ofList())
                // Custom cost factory to use during optimization
                .costFactory(null)
                .typeSystem(RelDataTypeSystem.DEFAULT)
                .build();

        return calciteFrameworkConfig;
    }

    public SimpleQueryPlanner(SchemaPlus schema) {
        FrameworkConfig calciteFrameworkConfig = getConfig(schema);
        this.planner = Frameworks.getPlanner(calciteFrameworkConfig);
    }

    public RelNode getLogicalPlan(String query) throws ValidationException, RelConversionException {
        SqlNode sqlNode;

        try {
            sqlNode = planner.parse(query);
        } catch (SqlParseException e) {
            throw new RuntimeException("Query parsing error.", e);
        }

        SqlNode validatedSqlNode = planner.validate(sqlNode);

        return planner.rel(validatedSqlNode).project();
    }

    public static void testSQLPlan() throws Exception {
        // Simple connection implementation for loading schema from sales.json
        CalciteConnection connection = new SimpleCalciteConnection();
        String salesSchema = Resources.toString(SimpleQueryPlanner.class.getResource("/sales.json"), Charset.defaultCharset());
        // ModelHandler reads the sales schema and load the schema to connection's root schema and sets the default schema
        new ModelHandler(connection, "inline:" + salesSchema);

        // Create the query planner with sales schema. conneciton.getSchema returns default schema name specified in sales.json
        SimpleQueryPlanner queryPlanner = new SimpleQueryPlanner(connection.getRootSchema().getSubSchema(connection.getSchema()));
        RelNode logicalPlan = queryPlanner.getLogicalPlan("select product from orders");
        System.out.println(RelOptUtil.toString(logicalPlan));
    }

    public static RelBuilder supplyRelBuilder(String model) throws Exception{
        CalciteConnection connection = new SimpleCalciteConnection();
        String salesSchema = Resources.toString(SimpleQueryPlanner.class.getResource("/"+model), Charset.defaultCharset());
        // ModelHandler reads the sales schema and load the schema to connection's root schema and sets the default schema
        new ModelHandler(connection, "inline:" + salesSchema);

        SchemaPlus schema = connection.getRootSchema().getSubSchema(connection.getSchema());

        FrameworkConfig config = getConfig(schema);

        final RelBuilder builder = RelBuilder.create(config);

        return builder;
    }


    public static void testBuilder() throws Exception{


        RelBuilder builder = supplyRelBuilder("sales.json");

        RexBuilder rexBuilder = builder.getRexBuilder();

        RelNode logicalScan = builder.scan("Orders").build();
        LogicalTupleWindow logicalWindow = LogicalTupleWindow.create(logicalScan, builder.literal(1000));

        RelNode node = builder
                .push(logicalWindow)
                .project(builder.field("ID"),builder.field("PRODUCT"))
                .filter(builder.call(SqlStdOperatorTable.GREATER_THAN,builder.field("ID"),builder.literal(3)))
                .build();

        node = LogicalDelta.create(node,builder.literal("RET"));


        System.out.println(RelOptUtil.toString(node));
    }

    public static void main(String[] args) throws Exception{

        testBuilder();
    }
}

