package com.rahasak.buckets

import java.io.ByteArrayInputStream

import io.minio.MinioClient
import org.apache.commons.io.IOUtils
import sun.misc.BASE64Decoder

object MinioOps extends App {

  // minio client with access key and secret key
  val minioClient = new MinioClient("http://localhost:9000",
    "rahasakkey",
    "rahasaksecret")

  /**
    * Put object into minio storage
    *
    * @param bucket bucket name
    * @param id     object id
    * @param blob   object blob
    */
  def put(bucket: String, id: String, blob: Array[Byte]): Unit = {
    // create bucket if not exists
    if (!minioClient.bucketExists(bucket)) {
      minioClient.makeBucket(bucket)
    }

    // put object
    val bais = new ByteArrayInputStream(blob)
    minioClient.putObject(bucket, id, bais, bais.available(), null, null, "binary/octet-stream")

    bais.close()
  }

  /**
    * Get object from minio storage
    *
    * @param bucket bucket name
    * @param id     object it
    */
  def get(bucket: String, id: String): Unit = {
    // get object as byte array
    val stream = minioClient.getObject(bucket, id)
    val blob = IOUtils.toByteArray(stream)
    println(blob.length)

    // get object stat
    val stat = minioClient.statObject(bucket, id)
    println(stat.bucketName())
  }

  /**
    * remove object from minio storage
    *
    * @param bucket bucket name
    * @param id     object it
    */
  def delete(bucket: String, id: String): Unit = {
    // remove object
    minioClient.removeObject(bucket, id)

    // get size of the bucket
    println(List(minioClient.listObjects(bucket)).size)
  }

  // create blob as a stream with base64 encoded string
  val payload =
    """
      |UmFoYXNhayBpcyBhIGhpZ2hseSBzY2FsYWJsZSBCbG9ja2NoYWluIHN5c3RlbSB3aGljaCBpcyB0YXJnZXRlZCBmb3IgYmlnIGRhdGEuIEl0IHV0a
      |WxpemVzIGEgZXZlbnR1YWwgY29uc2lzdGVudCBkaXN0cmlidXRlZCBkYXRhYmFzZSB+XGNpdGV7bGFrc2htYW4yMDEwY2Fzc2FuZHJhfSBhcyB1bm
      |Rlcmx5aW5nIGNvbnNlbnN1c35cY2l0ZXtsYW1wb3J0MTk5OHBhcnR9IGFuZCBzdG9yYWdlIHBsYXRmb3JtLiBFdmVudHVhbCBjb25zaXN0ZW50IGR
      |hdGFiYXNlcyBwcm9kdWNlIGhpZ2ggdHJhbnNhY3Rpb24gd3JpdGUgdGhyb3VnaHB1dH5cY2l0ZXtsYWtzaG1hbjIwMTBjYXNzYW5kcmF9IHdoZW4g
      |Y29tcGFyZWQgdG8gb3RoZXIgZGF0YWJhc2VzLg==
    """.stripMargin
  val blob = new BASE64Decoder().decodeBuffer(payload)

  // put object
  put("ops", "0122", blob)

  // get object
  get("ops", "0122")

  // delete object
  delete("ops", "0122")

}

