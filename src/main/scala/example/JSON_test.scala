package example

import os._

object JSON_Test{
    def main(args: Array[String]): Unit =  {
        val jsonString = os.read(os.pwd/"src"/"test"/"resources"/"phil.json")
        val data = ujson.read(jsonString)
    }
}