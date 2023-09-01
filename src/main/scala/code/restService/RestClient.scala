package code.restService

import zio.{ZIO, ZLayer}
import zio.http._

import code.lib.AppAux._
import code.model.Entities._

import java.time.Instant

trait RestClient {
  def reposByOrganization(organization: Organization): ZIO[zio.http.Client, Nothing, List[Repository]]
  def contributorsByRepo(organization: Organization, repo: Repository): ZIO[zio.http.Client, Nothing, List[Contributor]]
}

final case class RestClientLive() extends RestClient {

  def reposByOrganization(organization: Organization): ZIO[zio.http.Client, Nothing, List[Repository]] = {
    val resp = processResponseBody(s"https://api.github.com/orgs/$organization/repos") { responsePage =>
      val full_name_RE = s""","full_name":"$organization/([^"]+)",""".r
      val full_name_I = for (full_name_RE(full_name) <- full_name_RE.findAllIn(responsePage)) yield full_name
      val updated_at_RE = s""","updated_at":"([^"]+)",""".r
      val updated_at_I = for (updated_at_RE(updated_at) <- updated_at_RE.findAllIn(responsePage)) yield updated_at
      full_name_I.zip(updated_at_I).map(p => Repository(p._1, Instant.parse(p._2))).toList
    }
    resp//.tap(_ => ZIO.succeed(logger.info(s"[${Thread.currentThread().getName}] reposByOrganization($organization)")))
  }

  def contributorsByRepo(organization: Organization, repo: Repository): ZIO[zio.http.Client, Nothing, List[Contributor]] = {
    val resp = processResponseBody(s"https://api.github.com/repos/$organization/${repo.name}/contributors") { responsePage =>
      val login_RE = """"login":"([^"]+)"""".r
      val login_I = for (login_RE(login) <- login_RE.findAllIn(responsePage)) yield login
      val contributions_RE = """"contributions":([0-9]+)""".r
      val contributions_I = for (contributions_RE(contributions) <- contributions_RE.findAllIn(responsePage)) yield contributions
      val contributors_L = login_I.zip(contributions_I).map(p => Contributor(repo.name, p._1, p._2.toInt)).toList
      logger.info(s"repo='${repo.name}', # of contributors=${contributors_L.length}")
      contributors_L
    }
    resp//.tap(_ => ZIO.succeed(logger.info(s"[${Thread.currentThread().getName}] contributorsByRepo($organization, ${repo.name})")))
  }

  private def processResponseBody[T](url: String) (processPage: BodyString => List[T]): ZIO[zio.http.Client, Nothing, List[T]] = {

    def processResponsePage(processedPages: List[T], pageNumber: Int): ZIO[zio.http.Client, Nothing, List[T]] = {
      getResponseBody(s"$url?page=$pageNumber&per_page=100").orDie.flatMap {
        case Right(pageBody) if pageBody.length > 2 =>
          val processedPage = processPage(pageBody)
          processResponsePage(processedPages ++ processedPage, pageNumber + 1)
        case Right(_) =>
          ZIO.succeed(processedPages)
        case Left(error) =>
          logger.info(s"processResponseBody error - $error")
          ZIO.succeed(processedPages)
      }
    }

    processResponsePage(List.empty[T], 1)
  }

  private def getResponseBody(url: String): ZIO[zio.http.Client, Throwable, Either[BodyString, ErrorString]] = {

    def responseBody(body: BodyString, status: Status): Either[BodyString, ErrorString] =
      status match {
        case Status.Ok =>
          Right(body)
        case Status.Forbidden =>
          Left("API rate limit exceeded")
        case Status.NotFound =>
          Left("Non-existent organization")
        case _ =>
          Left(s"Unexpected StatusCode ${status.code}")
      }

    val token = if (gh_token != null) gh_token else ""
    for {
      response <- zio.http.Client.request(url, Method.GET, Headers("Authorization" -> token))
      bodyString <- response.body.asString
    } yield responseBody(bodyString, response.status)

  }

}

object RestClientLive {
  val layer =
    ZLayer.succeed(new RestClientLive())
}
