package com.cloudxlab.logparsing

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}

class SampleTest extends FunSuite with SharedSparkContext {
    test("Computing top5 URLs") {
        var line1 = "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0\" 200 1839"
        var line2 = "slppp6.intermind.net - - [01/Aug/1995:00:00:10 -0400] \"GET / HTTP/1.0\" 200 1687"
        var line3 = "ww-d1.proxy.aol.com - - [01/Aug/1995:00:25:35 -0400] \"GET /shuttle/technology/images/srb_16-small.gif HTTP/1.0\" 200 42732"
        var line4 = "piweba1y.prodigy.com - - [01/Aug/1995:00:24:46 -0400] \"GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0\" 200 1713"
        var line5 = "133.43.106.47 - - [01/Aug/1995:00:24:50 -0400] \"GET /images/NASA-logosmall.gif HTTP/1.0\" 200 786"
        var line6 = "ras15.srv.net - - [01/Aug/1995:00:24:57 -0400] \"GET /shuttle/missions/missions.html HTTP/1.0\" 200 8677"
        var line7 = "133.43.106.47 - - [01/Aug/1995:00:25:00 -0400] \"GET /images/MOSAIC-logosmall.gif HTTP/1.0\" 200 363"
        var line8 = "ras15.srv.net - - [01/Aug/1995:00:25:01 -0400] \"GET /images/launchmedium.gif HTTP/1.0\" 200 11853"
        var line9 = "www-d1.proxy.aol.com - - [01/Aug/1995:00:25:05 -0400] \"GET /shuttle/technology/sts-newsref/srb.html HTTP/1.0\" 200 49553"
        var line10 = "133.43.106.47 - - [01/Aug/1995:00:25:11 -0400] \"GET /shuttle/missions/missions.html HTTP/1.0\" 200 234"
        var line11 = "ix-sea6-23.ix.netcom.com - - [01/Aug/1995:00:25:12 -0400] \"GET /software/winvn/winvn.html HTTP/1.0\" 200 9866"
        var line12 = "csclass.utdallas.edu - - [01/Aug/1995:00:25:13 -0400] \"GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0\" 200 40960"
        var line13 = "ix-sea6-23.ix.netcom.com - - [01/Aug/1995:00:25:14 -0400] \"GET /software/winvn/winvn.gif HTTP/1.0\" 200 25218"
        var line14 = "ix-sea6-23.ix.netcom.com - - [01/Aug/1995:00:25:14 -0400] \"GET /images/construct.gif HTTP/1.0\" 200 1414"
        var line15 = "ix-sea6-23.ix.netcom.com - - [01/Aug/1995:00:25:14 -0400] \"GET /software/winvn/bluemarb.html HTTP/1.0\" 200 4441"
        
        val utils = new Utils

        val list = List(line1, line2, line3, line4, line5, line6, line7, line8, line9, line10, line11, line12, line13, line14, line15)
        val rdd = sc.parallelize(list);

        assert(rdd.count === list.length)   

        val records = utils.gettop5urls(rdd, sc)
        assert(records.length === 5)    
        assert(records(0)._1 == "/shuttle/missions/sts-69/mission-sts-69.html")
        assert(records(0)._2 == 3)
        assert(records(1)._1 == "/shuttle/missions/missions.html")
        assert(records(1)._2 == 2)
    }
    test("Computing http codes counts") {
        var line1 = "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0\" 200 1839"
        var line2 = "slppp6.intermind.net - - [01/Aug/1995:00:00:10 -0400] \"GET / HTTP/1.0\" - 1687"
        var line3 = "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0\" abc 1839"
        var line4 = "piweba1y.prodigy.com - - [01/Aug/1995:00:24:46 -0400] \"GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0\" 500 1713"
        var line5 = "133.43.106.47 - - [01/Aug/1995:00:24:50 -0400] \"GET /images/NASA-logosmall.gif HTTP/1.0\" 404 786"
        var line6 = "ras15.srv.net - - [01/Aug/1995:00:24:57 -0400] \"GET /shuttle/missions/missions.html HTTP/1.0\" 200 8677"
        var line7 = "133.43.106.47 - - [01/Aug/1995:00:25:00 -0400] \"GET /images/MOSAIC-logosmall.gif HTTP/1.0\" 200 363"
        var line8 = "ras15.srv.net - - [01/Aug/1995:00:25:01 -0400] \"GET /images/launchmedium.gif HTTP/1.0\" 200 11853"
        var line9 = "www-d1.proxy.aol.com - - [01/Aug/1995:00:25:05 -0400] \"GET /shuttle/technology/sts-newsref/srb.html HTTP/1.0\" 404 49553"
        var line10 = "133.43.106.47 - - [01/Aug/1995:00:25:11 -0400] \"GET /shuttle/missions/missions.html HTTP/1.0\" 500 234"
        var line11 = "ix-sea6-23.ix.netcom.com - - [01/Aug/1995:00:25:12 -0400] \"GET /software/winvn/winvn.html HTTP/1.0\" 200 9866"
        var line12 = "csclass.utdallas.edu - - [01/Aug/1995:00:25:13 -0400] \"GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0\" 404 40960"
        var line13 = "ix-sea6-23.ix.netcom.com - - [01/Aug/1995:00:25:14 -0400] \"GET /software/winvn/winvn.gif HTTP/1.0\" 500 25218"
        var line14 = "ix-sea6-23.ix.netcom.com - - [01/Aug/1995:00:25:14 -0400] \"GET /images/construct.gif HTTP/1.0\" xxx 1414"
        var line15 = "ix-sea6-23.ix.netcom.com - - [01/Aug/1995:00:25:14 -0400] \"GET /software/winvn/bluemarb.html HTTP/1.0\" 404 4441"
        
        val utils = new Utils

        val list = List(line1, line2, line3, line4, line5, line6, line7, line8, line9, line10, line11, line12, line13, line14, line15)
        val rdd = sc.parallelize(list);

        assert(rdd.count === list.length)   

        val records = utils.gethttpcodes(rdd, sc)
        assert(records.length === 3)    
        assert(records(0)._1 == "200")
        assert(records(0)._2 == 5)
        assert(records(1)._1 == "404")
        assert(records(1)._2 == 4)
        assert(records(2)._1 == "500")
        assert(records(2)._2 == 3)
    }
        test("Computing top/least time frames") {
        var line1 = "in24.inetnebr.com - - [01/Aug/1995:02:24:01 -0400] \"GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0\" 200 1839"
        var line2 = "slppp6.intermind.net - - [] \"GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0\" - 1687"
        var line3 = "in24.inetnebr.com - - [01/Aug/1995:03:00:01 -0400] \"GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0\" 200 1839"
        var line4 = "piweba1y.prodigy.com - - [01/Aug/1995:04:24:46 -0400] \"GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0\" 500 1713"
        var line5 = "133.43.106.47 - - [01/Aug/1995:02:24:50 -0400] \"GET /images/NASA-logosmall.gif HTTP/1.0\" 404 786"
        var line6 = "ras15.srv.net - - [01/Aug/1995:01:24:57 -0400] \"GET /shuttle/missions/missions.html HTTP/1.0\" 200 8677"
        var line7 = "133.43.106.47 - - [01/Aug/1995:02:25:00 -0400] \"GET /images/MOSAIC-logosmall.gif HTTP/1.0\" 200 363"
        var line8 = "ras15.srv.net - - [01/Aug/1995:00:25:01 -0400] \"GET /images/launchmedium.gif HTTP/1.0\" 200 11853"
        var line9 = "www-d1.proxy.aol.com - - [01/Aug/1995:00:25:05 -0400] \"GET /shuttle/technology/sts-newsref/srb.html HTTP/1.0\" 404 49553"
        var line10 = "133.43.106.47 - - [01/Aug/1995:02:25:11 -0400] \"GET /shuttle/missions/missions.html HTTP/1.0\" 500 234"
        var line11 = "ix-sea6-23.ix.netcom.com - - [01/Aug/1995:00:25:12 -0400] \"GET /software/winvn/winvn.html HTTP/1.0\" 200 9866"
        var line12 = "csclass.utdallas.edu - - [01/Aug/1995:00:25:13 -0400] \"GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0\" 404 40960"
        var line13 = "ix-sea6-23.ix.netcom.com - - [01/Aug/1995:01:25:14 -0400] \"GET /software/winvn/winvn.gif HTTP/1.0\" 500 25218"
        var line14 = "ix-sea6-23.ix.netcom.com - - [01/Aug/1995:00:25:14 -0400] \"GET /images/construct.gif HTTP/1.0\" 200 1414"
        var line15 = "ix-sea6-23.ix.netcom.com - - [01/Aug/1995:03:25:14 -0400] \"GET /software/winvn/bluemarb.html HTTP/1.0\" 404 4441"
        var line16 = "dialip129.gov.bc.ca - - [01/Aug/1995:01:26:23 -0400] \"GET /history/apollo/apollo-11/sounds/ HTTP/1.0\" 200 653"
        
        val utils = new Utils

        val list = List(line1, line2, line3, line4, line5, line6, line7, line8, line9, line10, line11, line12, line13, line14, line15, line16)
        val rdd = sc.parallelize(list);

        assert(rdd.count === list.length)   

        val records = utils.gettimeframes(rdd, sc,false)
        assert(records.length === 5)    
        assert(records(0)._1 == "01/Aug/1995:00")
        assert(records(0)._2 == 5)
        assert(records(1)._1 == "01/Aug/1995:02")
        assert(records(1)._2 == 4)
        assert(records(2)._1 == "01/Aug/1995:01")
        assert(records(2)._2 == 3)
    }

}
