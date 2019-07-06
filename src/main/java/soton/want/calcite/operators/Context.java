package soton.want.calcite.operators;

import soton.want.calcite.operators.physic.JoinOperator;
import soton.want.calcite.operators.physic.Operator;

import java.util.*;

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

    private int curStage;
    private int nextStage;
    HashSet<Operator> waitingOp = new HashSet<>();
    LinkedList<Operator> pendingOp = new LinkedList<>();


    private Map<Integer,LinkedList<Operator>> stages = new HashMap<>();

    public static Context getInstance() {
        return instance;
    }

    public long getCurrentTs() {
        return currentTs;
    }

    public void setCurrentTs(long currentTs) {
        this.currentTs = currentTs;
    }

    private void splitStages(List<Operator> tables){
        stages.put(1,new LinkedList<>());
        stages.get(1).addAll(tables);

        curStage = 1;
        nextStage = 2;

        while (true) {
            if (!stages.containsKey(curStage)){
                break;
            }
            for (Operator op : stages.get(curStage)){

                List<Operator> parents = op.getParents();
                if (parents==null){
                    continue;
                }
                if (! stages.containsKey(nextStage)){
                    stages.put(nextStage,new LinkedList<>());
                }

                while (parents.size()==1 && ! (parents.get(0) instanceof JoinOperator)){
                    pendingOp.add(parents.get(0));
                    parents = parents.get(0).getParents();
                }

                for (Operator parent : parents){
                    if (parent instanceof JoinOperator){
                        if (waitingOp.contains(parent)){
                            stages.get(nextStage).add(parent);
                            waitingOp.remove(op);
                        }
                        waitingOp.add(parent);
                    }else {
                        stages.get(nextStage).add(parent);
                    }
                }
            }
            stages.get(curStage).addAll(pendingOp);
            pendingOp = new LinkedList<>();
            curStage = nextStage;
            nextStage+=1;
        }
    }
    public void runOperator(List<Operator> tables){
        splitStages(tables);

        while (true){
            setCurrentTs(System.currentTimeMillis());
            for (int stageNum = 1; stageNum<curStage; stageNum++){
                LinkedList<Operator> operators = stages.get(stageNum);
                Iterator<Operator> iterator = operators.iterator();
                while (iterator.hasNext()){
                    Operator op = iterator.next();
                    op.run();
                }
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }
}
