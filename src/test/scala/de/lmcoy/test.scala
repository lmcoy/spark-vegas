import org.scalatest.{FlatSpec, Matchers}

class Test extends FlatSpec with Matchers {

    "test" should "test something" in {
        "test" should equal("test")
    }

    it should "test other things" in {
        "test2" should equal("test2")
    }

}