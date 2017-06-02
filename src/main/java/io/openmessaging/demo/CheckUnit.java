package io.openmessaging.demo;


public class CheckUnit<T> {
    private T content;
    private int checkCounter = 0;

    CheckUnit(T content) {
        this.content = content;
    }
}
