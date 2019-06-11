package soton.want.calcite.operators.physic;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import soton.want.calcite.operators.Tuple;

import java.util.LinkedList;

/**
 * @author want
 */
public class TableScanOperator extends AbstractOperator<TableScan> {

    /**
     * table source which produce tuple
     * {@link soton.want.calcite.plan.StreamTableFactory}
     * {@link soton.want.calcite.plan.UserTableFactory}
     *
     */
    RelOptTable table;

    Enumerator<Object[]> enumerator = null;

    LinkedList<Tuple> source;


    public TableScanOperator(TableScan logicalNode) {
        super(logicalNode);
        table = logicalNode.getTable();
        source = new LinkedList<>();

        Thread producer = new Thread(new Producer(),"thread-producer");
        producer.setDaemon(true);
        producer.start();

    }

    /**
     * daemon thread keeps taking tuple from the table source
     */
    private class Producer implements Runnable{

        @Override
        public void run() {
            Table unwrapTable = table.unwrap(Table.class);
            enumerator = ((ScannableTable)unwrapTable).scan(null).enumerator();

            while (true){
                Object[] row = null;
                if (enumerator.moveNext()){
                    row = enumerator.current();
                    synchronized (source){
                        source.addLast(new Tuple(row,table.getRowType()));
                    }
                }else {
                    break;
                }
            }
        }
    }

    @Override
    public Operator[] getChildren() {
        throw new UnsupportedOperationException();
    }

    public void setTable(RelOptTable table) {
        this.table = table;
    }


    @Override
    public void run() {

        synchronized (source){
            while (!source.isEmpty()){
                this.sink.addLast(Tuple.copy(source.pollFirst()));
            }
        }

        runParent();
    }


    public LinkedList<Tuple> getTupleQueue(){
        return source;
    }


}
