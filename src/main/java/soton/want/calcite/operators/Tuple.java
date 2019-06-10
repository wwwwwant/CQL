package soton.want.calcite.operators;

import org.apache.calcite.rel.type.RelDataType;

/**
 * @author want
 */
public class Tuple {

    public static Tuple copy(Tuple tuple) {
        return new Tuple(tuple.getValues(),tuple.getRowType(),tuple.getState());
    }

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

    public Tuple(Object[] values, RelDataType rowType) {
        this(values,rowType, State.ADD);
    }

    public Tuple(Object[] values,RelDataType rowType,State state){
        this.values = values;
        this.rowType = rowType;
        this.state = state;
    }

    public static Tuple join(Tuple t1, Tuple t2){
        Object[] v1 = t1.getValues();
        Object[] v2 = t2.getValues();
        Object[] value = new Object[v1.length+v2.length];

        return null;

    }

    public Tuple(Tuple tuple, State state){
        this(tuple.getValues(),tuple.getRowType(),state);
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
