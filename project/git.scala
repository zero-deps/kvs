package mws.kvs

import sbt._
import Keys._

import org.eclipse.jgit.lib.Repository
import org.eclipse.jgit.storage.file.FileRepositoryBuilder
import org.eclipse.jgit.api.{Git => PGit}
import java.io.File

import scala.util.Try

/**
 * Adds git tasks to the build.
 *
 * [todo] Since plugin is not supposed to be runned in the git-less environment 
 * consider to be replaces by command line interface.
 */
object Git extends AutoPlugin {
  override def trigger = allRequirements
  override def requires = plugins.JvmPlugin

  object autoImport {
    val git = inputKey[Int]("Git tasks.")
  }

  import autoImport._
  import complete.DefaultParsers._

  def gitInput = Def.inputTask {
    val s = state.value
    val args: Seq[String] = spaceDelimited("<arg>").parsed
    val ex = Project.extract(s)
    import ex._
    val dir = ex.get(baseDirectory)

    args.headOption match {
      case Some("pull") => pull(args drop 1)(dir, s.log)
      case _ =>
        classOf[org.eclipse.jgit.pgm.Main].getClassLoader match {
        case cl: java.net.URLClassLoader =>
        val cp = cl.getURLs map (_.getFile) mkString ":"
        val baos = new java.io.ByteArrayOutputStream
        val code = Fork.java(None, Seq("-classpath", cp, "org.eclipse.jgit.pgm.Main") ++ args, Some(dir), CustomOutput(baos))
        val result = baos.toString
        s.log.info(result)
        code
        case _ => -1
      }
    }
  }

  private def pull(args: Seq[String])(cwd: File, log: Logger = ConsoleLogger()): Int = {
    val git = JGit(cwd)
    git.view.pull().call();
    0
  }

  override def buildSettings = Seq(git <<= gitInput)
}

final class JGit(val repo: Repository){ val view = new PGit(repo) }
object JGit { def apply(base: File) = new JGit({ new FileRepositoryBuilder().setGitDir(new File(base, ".git")).build()}) }
