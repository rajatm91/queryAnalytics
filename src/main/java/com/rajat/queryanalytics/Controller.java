package com.rajat.queryanalytics;


import com.rajat.spark.QueryStatsEstimator$;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import scala.math.BigInt;

@RestController
public class Controller {

    @PostMapping("/estimateRows")
    public BigInt estimateRows(@RequestBody String query){
        return QueryStatsEstimator$.MODULE$.estimateRows(query);
    }
}
