import zio._

object ExampleApps extends ZIOAppDefault {

  // appMap
  val zio1 = ZIO.succeed("This is a string")
  val zio2 = zio1.map(_.toUpperCase)
  val zio3 = ZIO.fail("Boom!")
  val appMap =
    for {
      val1 <- zio1
      _ <- Console.printLine(s"zio1 succeeds with value ${val1}")
      val2 <- zio2
      _ <- Console.printLine(s"zio2 succeeds with value ${val2}")
      err <- zio3.mapError(_.toUpperCase())
      _ <- Console.printLine(s"zio3 fails with error ${err}!")
    } yield ()

  // appEcho
  val appEcho = Console.readLine("Echo ... please type something\n").flatMap(line => Console.printLine(line))

  // appHello
  val appHello =
    for {
      _ <- Console.printLine("Hello! What is your name?")
      name <- Console.readLine
      _ <- Console.printLine(s"Hello, ${name}, welcome to ZIO!")
    } yield ()

  // appReadInt
  val readInt: ZIO[Any, Throwable, Int] =
    for {
      line <- Console.readLine
      int <- ZIO.from(line.toInt)
    } yield int
  val readIntOrRetry: ZIO[Any, Nothing, Int] =
    readInt
      .orElse(Console.printLine("Please enter a valid integer")
        .zipRight(readIntOrRetry).orDie)
  val appReadInt =
    for {
      _ <- Console.printLine("Please enter an integer")
      i <- readIntOrRetry
      _ <- Console.printLine(s"$i entered")
    } yield i

  override def run = appReadInt

}

import java.io.IOException

object ExampleApps2 extends ZIOAppDefault {

  val helloWorld = ZIO.succeed(print("Hello ")).zipRight(ZIO.from(print("World!")))
  val helloWorld2 = Console.print("Hello ").zipRight(Console.print("World!"))

  val primaryOrBackupData: ZIO[Any, IOException, String] =
    ZIO.readFile("primary.data").orElse(ZIO.readFile("backup.data"))

  override def run = helloWorld

}
