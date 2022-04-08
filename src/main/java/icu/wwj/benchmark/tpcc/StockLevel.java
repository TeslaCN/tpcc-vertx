package icu.wwj.benchmark.tpcc;

class StockLevel {
   
    /* terminal input data */
    final int w_id;
    final int d_id;
    final int threshold;

    /* terminal output data */
    int low_stock;

    public StockLevel(int w_id, int d_id, int threshold) {
        this.w_id = w_id;
        this.d_id = d_id;
        this.threshold = threshold;
    }
}
