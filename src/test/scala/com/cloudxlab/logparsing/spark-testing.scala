package com.cloudxlab.logparsing

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}

class SampleTest extends FunSuite with SharedSparkContext {
    test("Computing top10 URLs") {
        var line1 = "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0\" 200 1839"
        var line2 = "slppp6.intermind.net - - [01/Aug/1995:00:00:10 -0400] \"GET / HTTP/1.0\" 200 1687"

        val utils = new Utils

        val list = List(line1, line2)
        val rdd = sc.parallelize(list);

        assert(rdd.count === list.length)   

        val records = utils.gettop10urls(rdd, sc)
        assert(records.length === 1)    
        assert(records(0)._1 == "/shuttle/missions/sts-69/mission-sts-69.html")
    }

}
