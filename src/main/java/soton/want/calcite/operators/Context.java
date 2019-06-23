package soton.want.calcite.operators;

/**
 * @author want
 */
public class Context {
    private static final Context instance = new Context();
    private Context(){}

    /**
     * Window end timestamp
     */
    private long currentTs;


    public static Context getInstance() {
        return instance;
    }

    public long getCurrentTs() {
        return currentTs;
    }

    public void setCurrentTs(long currentTs) {
        this.currentTs = currentTs;
    }
}
