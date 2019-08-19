package soton.want.calcite.operators;

import org.apache.calcite.rel.type.RelDataType;

/**
 * @author want
 */
public class Tuple {


    /**
     * represent the state of the tuple either added or deleted
     */
    public static enum State{
        ADD{
            @Override
            public String toString() {
                return "ADD";
            }
        },
        DEL{
            @Override
            public String toString() {
                return "DEL";
            }
        }
    }

    private Object[] values;
    private RelDataType rowType;
    private State state;
    private long ts;

    public Tuple(Object[] values, RelDataType rowType) {
        this(values,rowType, State.ADD);
    }


    public static Tuple copy(Tuple tuple) {
        return new Tuple(tuple.getValues().clone(),tuple.getRowType(),tuple.getState(),tuple.getTs());
    }


    public Tuple(Object[] values,RelDataType rowType,State state, long ts){
        this.values = values;
        this.rowType = rowType;
        this.state = state;
        this.ts = ts;
    }
    public Tuple(Object[] values,RelDataType rowType,State state){
        this(values,rowType,state,System.currentTimeMillis());
    }


    public Tuple(Tuple tuple, State state){
        this(tuple.getValues().clone(),tuple.getRowType(),state,tuple.getTs());
    }

    public Object get(int index){
        if (index>values.length){
            throw new IndexOutOfBoundsException();
        }
        return values[index];
    }

    public void set(int index, Object val){
        values[index] = val;
    }

    public int getFieldCount(){ return values.length;}

    public Object getField(int index){
        return values[index];
    }

    public Object[] getValues() {
        return values;
    }

    public RelDataType getRowType() {
        return rowType;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj==this){
            return true;
        }
        if (obj instanceof Tuple){
            Tuple t = (Tuple) obj;
            if (t.getRowType().equals(this.rowType)){
                Object[] vals = t.getValues();
                for (int i=0;i<values.length;i++){
                    if (!values[i].equals(vals[i])){
                        return false;
                    }
                }
            }
            return true;
        }
        return false;

    }
}
