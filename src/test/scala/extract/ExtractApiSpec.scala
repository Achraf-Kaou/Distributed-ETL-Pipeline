package extract

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import org.scalatest.matchers.should.Matchers
import support.SparkSessionTestWrapper

class ExtractApiSpec extends SparkSessionTestWrapper with Matchers {

  private def withServer(body: String, status: Int = 200)(testFn: String => Unit): Unit = {
    val server = HttpServer.create(new InetSocketAddress(0), 0)
    server.createContext("/payload", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        val bytes = body.getBytes(StandardCharsets.UTF_8)
        exchange.sendResponseHeaders(status, bytes.length)
        val os = exchange.getResponseBody
        os.write(bytes)
        os.close()
      }
    })
    server.start()
    try testFn(s"http://127.0.0.1:${server.getAddress.getPort}/payload")
    finally server.stop(0)
  }

  test("ExtractApi.read supports mocked API responses with root field expansion") {
    // Arrange
    val response =
      """{
        |  "data": [
        |    {"customer_id": "C001", "country": "usa"},
        |    {"customer_id": "C002", "country": "UK"}
        |  ]
        |}
        |""".stripMargin

    withServer(response) { url =>
      // Act
      val df = ExtractApi.read(spark, url = url, rootField = "data")

      // Assert
      df.count() shouldEqual 2L
      df.columns should contain allOf ("customer_id", "country")
    }
  }

  test("ExtractApi.read surfaces HTTP failures deterministically") {
    // Arrange
    withServer("{\"error\":\"boom\"}", status = 500) { url =>
      // Act / Assert
      val error = intercept[Exception] {
        ExtractApi.read(spark, url = url)
      }
      error.getMessage should include ("status code 500")
    }
  }
}
