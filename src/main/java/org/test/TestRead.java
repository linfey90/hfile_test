package org.test;

public class TestRead {
    //java -jar HfileTest-1.0-SNAPSHOT-shaded.jar /user/data/hfile2/none_1kw 1 10000000
    public static void main(String[] args) {
        String filePath = args[0];
        int nums = Integer.parseInt(args[1]);
        int count = Integer.parseInt(args[2]);
        for(int i=0; i< nums;i++){
            new Thread(new MyMulRead(filePath, count)).start();
        }
    }
}
