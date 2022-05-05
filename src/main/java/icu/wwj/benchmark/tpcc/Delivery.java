package icu.wwj.benchmark.tpcc;

import java.time.LocalDateTime;

class Delivery {

    final int w_id;
    
    int o_carrier_id;
    
    LocalDateTime ol_delivery_d;
    
    final int[] delivered_o_id = new int[10];

    Delivery(int w_id) {
        this.w_id = w_id;
    }
}
