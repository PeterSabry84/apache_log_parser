package com.cloudxlab.logparsing

import org.scalatest.FlatSpec

class LogParserSpec extends FlatSpec {

  "extractURL" should "Extract Accessed URL" in {
    val utils = new Utils
    var line = "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0\" 200 1839"
    var url = utils.extractURL(line)
    assert(url == "/shuttle/missions/sts-69/mission-sts-69.html")
  }
  "extractHTTPcode" should "Extract HTTP Code" in {
    val utils = new Utils
    var line = "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0\" 200 1839"
    var httpCode = utils.extractHTTPcode(line)
    assert(httpCode == "200")
  }
    "extractTimeFrames" should "Extract Time frame down to hours" in {
    val utils = new Utils
    var line = "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0\" 200 1839"
    var timeFrame = utils.extractTimeFrames(line)
    assert(timeFrame == "01/Aug/1995:00")
  }

  "containsURL" should "Check if URL exists in the log line" in {
    val utils = new Utils
    var line1 = "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0\" 200 1839"

    assert(utils.containsURL(line1))
    
    var line2 = "uplherc.upl.com - - [01/Aug/1995:00:00:07 -0400] \"GET / HTTP/1.0\" 304 0"
    assert(!utils.containsURL(line2))
  }
  "containsCode" should "Check if HTTP Code exists in the log line" in {
    val utils = new Utils
    var line1 = "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0\" 200 1839"

    assert(utils.containsCode(line1))
    
    var line2 = "uplherc.upl.com - - [01/Aug/1995:00:00:07 -0400] \"GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0\" xxx 0"
    assert(!utils.containsCode(line2))
  }
  "containsTimeFrame" should "Check if Time frame exists in the log line" in {
    val utils = new Utils
    var line1 = "in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] \"GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0\" 200 1839"

    assert(utils.containsTimeFrame(line1))
    
    var line2 = "uplherc.upl.com - - [] \"GET /shuttle/missions/sts-69/mission-sts-69.html HTTP/1.0\" 304 0"
    assert(!utils.containsTimeFrame(line2))
  }
}
