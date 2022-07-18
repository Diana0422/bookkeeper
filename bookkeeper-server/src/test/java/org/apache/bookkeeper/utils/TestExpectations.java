package org.apache.bookkeeper.utils;

public class TestExpectations {
    private boolean test1;
    private boolean test2;
    private boolean test3;
    private boolean test4;

    public TestExpectations(boolean test1, boolean test2, boolean test3, boolean test4) {
        this.test1 = test1;
        this.test2 = test2;
        this.test3 = test3;
        this.test4 = test4;
    }

    public boolean isTest1() {
        return test1;
    }

    public void setTest1(boolean test1) {
        this.test1 = test1;
    }

    public boolean isTest2() {
        return test2;
    }

    public void setTest2(boolean test2) {
        this.test2 = test2;
    }

    public boolean isTest3() {
        return test3;
    }

    public void setTest3(boolean test3) {
        this.test3 = test3;
    }

    public boolean isTest4() {
        return test4;
    }

    public void setTest4(boolean test4) {
        this.test4 = test4;
    }


}