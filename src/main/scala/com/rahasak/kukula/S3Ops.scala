package com.rahasak.buckets

import java.io.ByteArrayInputStream

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import org.apache.commons.io.IOUtils
import sun.misc.BASE64Decoder

object S3Ops extends App {

  // s3 client with minio endpoint
  val s3Credential = new BasicAWSCredentials("rahasakkey", "rahasaksecret")
  val s3Client = AmazonS3ClientBuilder
    .standard()
    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:9000", Regions.US_EAST_1.name()))
    .withPathStyleAccessEnabled(true)
    .withCredentials(new AWSStaticCredentialsProvider(s3Credential))
    .build()

  /**
    * Put object into minio storage
    *
    * @param bucket bucket name
    * @param id     object id
    * @param blob   object blob
    */
  def put(bucket: String, id: String, blob: Array[Byte]): Unit = {
    // create bucket if not exists
    if (!s3Client.doesBucketExistV2(bucket)) {
      s3Client.createBucket(bucket)
    }

    // list buckets
    println(s3Client.listBuckets())

    // put object
    val bais = new ByteArrayInputStream(blob)
    val metadata = new ObjectMetadata()
    metadata.setContentLength(bais.available())
    s3Client.putObject(bucket, id, bais, metadata)

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
    val obj = s3Client.getObject(bucket, id)
    val blob = IOUtils.toByteArray(obj.getObjectContent)
    println(blob.length)

    // get object stat
    println(obj.getBucketName)
    println(obj.getObjectMetadata.getContentLength)
  }

  /**
    * remove object from minio storage
    *
    * @param bucket bucket name
    * @param id     object it
    */
  def delete(bucket: String, id: String): Unit = {
    // remove object
    s3Client.deleteObject(bucket, id)

    // get size of the bucket
    println(List(s3Client.listObjects(bucket)).size)
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


  // put object to sub folder
  put("rahasak", "labs/3422", blob)

  // get object in sub folder
  get("rahasak", "labs/3422")

  // delete object in sub folder
  delete("rahasak", "labs/3422")

}
