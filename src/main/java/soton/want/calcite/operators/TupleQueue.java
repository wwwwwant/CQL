package soton.want.calcite.operators;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * @author want
 */
public class TupleQueue {

    private LinkedList<Tuple> queue;

    public TupleQueue(){
        this.queue = new LinkedList<>();
    }


    public LinkedList<Tuple> getQueue() {
        return queue;
    }

    public void addLast(Tuple tuple){
        queue.addLast(tuple);
    }

    public Tuple pollFirst(){
        return queue.pollFirst();
    }

    public int size(){
        return queue.size();
    }

    public Tuple remove(int index){
        return queue.remove(index);
    }

    public Tuple get(int index){
        return queue.get(index);
    }

    public Iterator<Tuple> iterator(){
        return queue.iterator();
    }

    public boolean isEmpty(){
        return queue.isEmpty();
    }





}
