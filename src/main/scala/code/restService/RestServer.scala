package code.restService

import zio._
import zio.http._
import zio.json._

import code.lib.AppAux._
import code.model.Entities._

import java.time.{Duration, Instant}
import java.util.Date

// RestServer layer
trait RestServer {
  val runServer: ZIO[Any, Throwable, ExitCode]
}

final case class RestServerLive(restClient: RestClient, restServerCache: RestServerCache) extends RestServer {

  private val sdf = new java.text.SimpleDateFormat("dd-MM-yyyy hh:mm:ss")

  // retrieve contributors by repo using ZIO-http and a Redis cache
  private def contributorsByOrganization(organization: Organization, groupLevel: String, minContribs: Int):
    ZIO[zio.http.Client, Throwable, List[Contributor]] = for {
      initialInstant <- Clock.instant
      _ <- ZIO.succeed(logger.info(s"Starting ContribsGH2-Z REST API call at ${sdf.format(Date.from(initialInstant))} - organization='$organization'"))
      repos <- restClient.reposByOrganization(organization)
      _ <- ZIO.succeed(logger.info(s"# of repos=${repos.length}"))
      contributorsDetailed <- contributorsDetailedZIOWithCache(organization, repos)
      contributors = groupContributors(organization, groupLevel, minContribs, contributorsDetailed)
      finalInstant <- Clock.instant
      _ <- ZIO.succeed(logger.info(s"Finished ContribsGH2-Z REST API call at ${sdf.format(Date.from(finalInstant))} - organization='$organization'"))
      _ <- ZIO.succeed(logger.info(f"Time elapsed from start to finish: ${Duration.between(initialInstant, finalInstant).toMillis / 1000.0}%3.2f seconds"))
    } yield contributors

  private def contributorsDetailedZIOWithCache(organization: Organization, repos: List[Repository]):
    ZIO[zio.http.Client, Throwable, List[Contributor]] = {

    val (reposUpdatedInCache, reposNotUpdatedInCache) = repos.partition(restServerCache.repoUpdatedInCache(organization, _))
    val contributorsDetailed_L_1: List[List[Contributor]] =
      reposUpdatedInCache.map { repo =>
        restServerCache.retrieveContributorsFromCache(organization, repo)
      }
    val contributorsDetailed_L_Z_2 =
      reposNotUpdatedInCache.map { repo =>
        restClient.contributorsByRepo(organization, repo)
      }

    // retrieve contributors by repo in parallel
    val contributorsDetailed_Z_L_2 = ZIO.collectAllPar(contributorsDetailed_L_Z_2).withParallelism(8)
    // retrieve contributors by repo sequentially
    //val contributorsDetailed_Z_L_2 = ZIO.collectAll(contributorsDetailed_L_Z_2)

    for {
      contributorsDetailed_L_2 <- contributorsDetailed_Z_L_2
      _ <- ZIO.succeed(restServerCache.updateCache(organization, reposNotUpdatedInCache, contributorsDetailed_L_2))
    } yield (contributorsDetailed_L_1 ++ contributorsDetailed_L_2).flatten

  }

  // group - sort list of contributors
  private def groupContributors(organization: Organization,
                                groupLevel: String,
                                minContribs: Int,
                                contributorsDetailed: List[Contributor]): List[Contributor] = {
    val (contributorsGroupedAboveMin, contributorsGroupedBelowMin) = contributorsDetailed.
      map(c => if (groupLevel == "repo") c else c.copy(repo = s"All $organization repos")).
      groupBy(c => (c.repo, c.contributor)).
      view.mapValues(_.foldLeft(0)((acc, elt) => acc + elt.contributions)).
      map(p => Contributor(p._1._1, p._1._2, p._2)).
      partition(_.contributions >= minContribs)
    val contributorsGrouped = {
      (
        contributorsGroupedAboveMin
          ++
          contributorsGroupedBelowMin.
            map(c => c.copy(contributor = "Other contributors")).
            groupBy(c => (c.repo, c.contributor)).
            view.mapValues(_.foldLeft(0)((acc, elt) => acc + elt.contributions)).
            map(p => Contributor(p._1._1, p._1._2, p._2))
        ).toList.sortWith { (c1: Contributor, c2: Contributor) =>
        if (c1.repo != c2.repo) c1.repo < c2.repo
        else if (c1.contributor == "Other contributors") false
        else if (c1.contributions != c2.contributions) c1.contributions >= c2.contributions
        else c1.contributor < c2.contributor
      }
    }
    contributorsGrouped
  }

  // ZIO-Http definition of the endpoint for the REST service
  private val contribsGH2ZApp: Http[zio.http.Client, Nothing, zio.http.Request, zio.http.Response] =
    Http.collectZIO[Request] {
      case req@(Method.GET -> !! / "org" / organization / "contributors") =>
        val glS = req.url.queryParams.get("group-level").getOrElse(Chunk[String]()).asString
        val gl = if (glS.trim == "") "organization" else glS
        val mcS = req.url.queryParams.get("min-contribs").getOrElse(Chunk[String]()).asString
        val mc = mcS.toIntOption.getOrElse(0)
        contributorsByOrganization(organization, gl, mc).orDie.map(l => Response.json(l.toJson))
    }

  // ZIO-Http definition of the server for the REST service
  private val port: Int = 8080
  val runServer: ZIO[Any, Throwable, ExitCode] = for {
    _ <- Console.printLine(s"Starting server on http://0.0.0.0:$port")
    _ <- Server.serve(contribsGH2ZApp).provide(Server.defaultWithPort(port), zio.http.Client.default)
  } yield ExitCode.success

}

object RestServerLive {
  val layer =
    ZLayer.fromFunction(RestServerLive(_, _))
}

// REST service implementation as a running instance of a ZIO-Http server, with all dependencies provided as ZIO layers
object ContribsGH2Z extends ZIOAppDefault {

  override val run = {
    ZIO.serviceWithZIO[RestServer](_.runServer).
      provide(
        RestClientLive.layer,
        RestServerLive.layer,
        RestServerCacheLive.layer,
        RedisServerClientLive.layer
      )
  }

}
