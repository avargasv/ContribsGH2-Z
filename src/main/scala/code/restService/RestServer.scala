package code.restService

import zio._
import zio.http._
import zio.json._

import code.lib.AppAux._
import code.model.Entities._

import java.time.{Duration, Instant}
import java.util.Date

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
    _ <- Console.printLine(s"Starting server on http://localhost:$port")
    _ <- Server.serve(contribsGH2ZApp).provide(Server.defaultWithPort(port), zio.http.Client.default)
  } yield ExitCode.success

}

object RestServerLive {
  val layer =
    ZLayer.fromFunction(RestServerLive(_, _))
}

import redis.embedded.RedisServer
import redis.clients.jedis.Jedis
import scala.jdk.CollectionConverters._

trait RestServerCache {
  def repoUpdatedInCache(org:Organization, repo: Repository): Boolean
  def retrieveContributorsFromCache(org:Organization, repo: Repository): List[Contributor]
  def updateCache(organization: Organization, reposNotUpdatedInCache: List[Repository], contributors_L: List[List[Contributor]]): Unit
}

case class RestServerCacheLive(redisServerClient: RedisServerClient) extends RestServerCache {

  private def contribToString(c: Contributor) = c.contributor.trim + ":" + c.contributions

  private def stringToContrib(r: Repository, s: String) = {
    val v = s.split(":").toVector
    Contributor(r.name, v(0).trim, v(1).trim.toInt)
  }

  private def buildRepoK(o: Organization, r: Repository) = o.trim + "-" + r.name

  // return true if the repository was updated in the Redis cache after it was updated on the GitHub server
  def repoUpdatedInCache(org: Organization, repo: Repository): Boolean = {
    val repoK = buildRepoK(org, repo)
    redisServerClient.redisClient.lrange(repoK, 0, 0).asScala.toList match {
      case s :: _ =>
        val cachedUpdatedAt = Instant.parse(s.substring(s.indexOf(":") + 1))
        cachedUpdatedAt.compareTo(repo.updatedAt) >= 0
      case _ => false
    }
  }

  def retrieveContributorsFromCache(org:Organization, repo: Repository): List[Contributor] = {
    val repoK = buildRepoK(org, repo)
    val res = redisServerClient.redisClient.lrange(repoK, 1, redisServerClient.redisClient.llen(repoK).toInt - 1).
      asScala.toList
    logger.info(s"repo '$repoK' retrieved from cache, # of contributors=${res.length}")
    res.map(s => stringToContrib(repo, s))
  }

  private def saveContributorsToCache(org: Organization, repo: Repository, contributors: List[Contributor]) = {
    val repoK = buildRepoK(org, repo)
    redisServerClient.redisClient.del(repoK)
    logger.info(s"repo '$repoK' stored in cache")
    contributors.foreach { c: Contributor =>
      redisServerClient.redisClient.lpush(repoK, contribToString(c))
    }
    redisServerClient.redisClient.lpush(repoK, s"updatedAt:${repo.updatedAt.toString}")
  }

  // update cache with non-existent or recently-modified repos
  def updateCache(organization: Organization, reposNotUpdatedInCache: List[Repository], contributors_L: List[List[Contributor]]): Unit = {
    contributors_L.foreach { contribs_L =>
      if (contribs_L.length > 0) {
        val repoK = organization.trim + "-" + contribs_L.head.repo
        reposNotUpdatedInCache.find(r => (organization.trim + "-" + r.name) == repoK) match {
          case Some(repo) =>
            saveContributorsToCache(organization, repo, contribs_L)
          case None =>
            ()
        }
      }
    }
  }

}

object RestServerCacheLive {
  val layer =
    ZLayer.fromFunction(RestServerCacheLive(_))
}

trait RedisServerClient {
  val redisServer: RedisServer
  val redisClient: Jedis
}

case class RedisServerClientLive() extends RedisServerClient {
  val redisServer = new RedisServer(6379)
  try {
    redisServer.start()
  } catch {
    // just use it if already started
    case _: Throwable => ()
  }
  val redisClient = new Jedis()
}

object RedisServerClientLive {
  def releaseRSCAux(rsc: RedisServerClientLive): Unit = {
    rsc.redisClient.flushAll()
    rsc.redisClient.close()
    rsc.redisServer.stop()
    logger.info("Cache cleared and Redis server stopped!")
  }
  def acquireRSC: ZIO[Any, Nothing, RedisServerClientLive] = ZIO.succeed(new RedisServerClientLive())
  def releaseRSC(rsc: RedisServerClientLive): ZIO[Any, Nothing, Unit] = ZIO.succeed(releaseRSCAux(rsc))

  val RedisServerClientLive = ZIO.acquireRelease(acquireRSC)(releaseRSC)
  val layer =
    ZLayer.scoped(RedisServerClientLive)
}

object ContribsGH2Z extends ZIOAppDefault {

  // REST service implementation as a running instance of a ZIO-Http server, with all dependencies provided as ZIO layers
  // ("end-of-the-world" execution by the ZIO run-time of a ZIO service defined by composition of effects as values)
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
