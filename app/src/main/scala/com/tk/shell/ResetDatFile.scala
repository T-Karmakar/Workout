package com.tk.shell

class ResetDatFile {

}

import java.io.File
import java.io.PrintWriter

object ResetDatFile {
  def main(args: Array[String]): Unit = {
    val filePath = "path/to/your/file.dat"

    // Call the resetFile method to clear the file
    resetFile(filePath)
  }

  private def resetFile(filePath: String): Unit = {
    val file = new File(filePath)

    // Create a new PrintWriter to overwrite the file
    val writer = new PrintWriter(file)

    // Close the writer immediately to clear the file contents
    writer.close()

    println(s"File '${file.getAbsolutePath}' has been reset.")
  }
}

