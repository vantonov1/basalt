package com.github.vantonov1.basalt.repo;

public class Pair<F, S> implements Cloneable {
    private F first;
    private S second;

    public Pair() {
    }

    @Override
    public Pair<F, S> clone() {
        Pair<F, S> pair = new Pair<F, S>(first, second);
        return pair;
    }

    public Pair(F first, S second) {
        this.first = first;
        this.second = second;
    }

    public F getFirst() {
        return first;
    }

    public S getSecond() {
        return second;
    }

    public void setFirst(F first) {
        this.first = first;
    }

    public void setSecond(S second) {
        this.second = second;
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || (obj != null && this.getClass().equals(obj.getClass())
                && (first == null ? ((Pair) obj).first == null : first.equals(((Pair) obj).first))
                && (second == null ? ((Pair) obj).second == null : second.equals(((Pair) obj).second)));
    }

    @Override
    public int hashCode() {
        return (first == null ? 0 : first.hashCode()) + (second == null ? 0 : second.hashCode());
    }

    @Override
    public String toString() {
        return "Pair{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }
}
