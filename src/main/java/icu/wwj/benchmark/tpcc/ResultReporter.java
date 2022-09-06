package icu.wwj.benchmark.tpcc;

import lombok.Getter;

import java.util.concurrent.atomic.AtomicLong;

@Getter
public class ResultReporter {

    private final AtomicLong[] totalCount;

    private final AtomicLong[] newOrderCounts;

    public ResultReporter(int terminals) {
        totalCount = new AtomicLong[terminals];
        newOrderCounts = new AtomicLong[terminals];
        for (int i = 0; i < terminals; i++) {
            totalCount[i] = new AtomicLong();
            newOrderCounts[i] = new AtomicLong();
        }
    }
    
    public long sumTotalCount() {
        long result = 0;
        for (AtomicLong each : totalCount) {
            result += each.get();
        }
        return result;
    }
    
    public long sumNewOrderCount() {
        long result = 0;
        for (AtomicLong each : newOrderCounts) {
            result += each.get();
        }
        return result;
    }
}
