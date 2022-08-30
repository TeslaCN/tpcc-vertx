package icu.wwj.benchmark.tpcc;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@AllArgsConstructor
@Data
public class TerminalResult implements Serializable {

    private long totalCount;

    private long newOrderCount;
}
