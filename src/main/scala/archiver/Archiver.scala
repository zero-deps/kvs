package mws.rng.arch

import scala.collection.JavaConversions._
import java.io.{File, OutputStream, FileInputStream, BufferedInputStream, FileOutputStream, InputStream, IOException }
import java.util.zip.{ZipEntry, ZipFile, ZipOutputStream, ZipInputStream}

object Archiver {

	val BUFSIZE = 4096
  	val buffer = new Array[Byte](BUFSIZE)

	def zip(path: String) = {
		val zos = new ZipOutputStream(new FileOutputStream(s"$path.zip"))
		new File(path).listFiles.foreach{ file => 
			println(s"added to arch $file")
			zos.putNextEntry(new ZipEntry(file.getName))
			val in = new BufferedInputStream(new FileInputStream(file))
    		var b = in.read()
		    	while (b > -1) {
		      	zos.write(b)
		    	b = in.read()
   			}
    		in.close()
    		zos.closeEntry()
  		}
  		zos.close()
	}

	def unZipIt(zipFile: String, outputFolder: String): Unit = {
		val buffer = new Array[Byte](1024)

	    try {
		val folder = new File(outputFolder);
	      val zis: ZipInputStream = new ZipInputStream(new FileInputStream(zipFile));
	      var ze: ZipEntry = zis.getNextEntry();
			while (ze != null) {
			val fileName = ze.getName();
	        val newFile = new File(outputFolder + File.separator + fileName);
	        new File(newFile.getParent()).mkdirs();
	        val fos = new FileOutputStream(newFile);
	        var len: Int = zis.read(buffer);
	        while (len > 0) {
	          fos.write(buffer, 0, len)
	          len = zis.read(buffer)
	        }
	        fos.close()
	        ze = zis.getNextEntry()
	      }
	      zis.closeEntry()
	      zis.close()
	    } catch {
	      case e: IOException => println("exception caught: " + e.getMessage)
	    }
  }
}