package soton.want.calcite.plan;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.*;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;

/**
 * @author want
 */
public class StreamTableFactory implements TableFactory<Table>{

    @Override
    public Table create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
        return new InfiniteOrdersTable();
    }


    private abstract static class BaseOrderStreamTable implements ScannableTable {
        protected final RelProtoDataType protoRowType = a0 -> a0.builder()
//                .add("ROWTIME", SqlTypeName.TIMESTAMP)
                .add("ID", SqlTypeName.INTEGER)
                .add("USERID",SqlTypeName.INTEGER)
                .add("PRODUCTID",SqlTypeName.INTEGER)
                .add("PRODUCTNAME", SqlTypeName.VARCHAR, 10)
                .add("UNITS", SqlTypeName.DOUBLE)
                .build();

        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return protoRowType.apply(typeFactory);
        }

        public Statistic getStatistic() {
            return Statistics.of(100d, ImmutableList.of(),
                    RelCollations.createSingleton(0));
        }

        public Schema.TableType getJdbcTableType() {
            return Schema.TableType.TABLE;
        }

        @Override public boolean isRolledUp(String column) {
            return false;
        }

        @Override public boolean rolledUpColumnValidInsideAgg(String column,
                                                              SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
            return false;
        }
    }

    /**
     * Table representing an infinitely larger ORDERS stream.
     */
    public static class InfiniteOrdersTable extends BaseOrderStreamTable
            implements StreamableTable {
        public Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(() -> new Iterator<Object[]>() {
                Random random = new Random();
                private final String[] items = {"paint", "paper", "brush"};
                private final double[] prices = {10,20,30};
                private int counter = 0;

                public boolean hasNext() {
                    return true;
                }

                public Object[] next() {
                    final int index = counter++;
                    try {
                        Thread.sleep(600);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    return new Object[]{
                            index, index%2+1,index%items.length, items[index % items.length], prices[index % items.length]};
                }

                public void remove() {
                    throw new UnsupportedOperationException();
                }
            });
        }

        public Table stream() {
            return this;
        }
    }
}
