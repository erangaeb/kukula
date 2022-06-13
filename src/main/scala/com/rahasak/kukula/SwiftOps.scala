package com.rahasak.buckets

import java.io.ByteArrayInputStream

import org.javaswift.joss.client.factory.{AccountFactory, AuthenticationMethod}
import sun.misc.BASE64Decoder

object SwiftOps extends App {
  // create account in TestAuth mode with default username and password
  // TestAuth use basic authentication
  val account = new AccountFactory()
    .setAuthenticationMethod(AuthenticationMethod.KEYSTONE_V3)
    .setUsername("admin")
    .setPassword("passw0rd")
    .setTenantName("admin")
    .setDomain("default")
    //.setAuthUrl("http://127.0.0.1:32770/v2.0/tokens")
    .setAuthUrl("http://127.0.0.1:35357/v3/auth/tokens")
    .createAccount()
  println(account.list().size())

  /**
    * put object into container
    *
    * @param container container name
    * @param id        object id
    * @param blob      object blob
    */
  def put(container: String, id: String, blob: Array[Byte]): Unit = {
    // create container if not exists
    val con = account.getContainer(container)
    if (!con.exists()) {
      con.create()
      //con.makePublic()
    }

    // put object into container as stream
    val bais = new ByteArrayInputStream(blob)
    val obj = con.getObject(id)
    obj.uploadObject(bais)

    // get object list count in the container
    println(con.list().size())
  }

  /**
    * get object from container
    *
    * @param container container name
    * @param id        object id
    */
  def get(container: String, id: String): Unit = {
    val con = account.getContainer(container)
    val obj = con.getObject(id)
    obj.downloadObject()

    // view object info
    println(obj.getContentLength)
    println(obj.getContentType)
    println(obj.getLastModified)
  }

  /**
    * delete object from container
    *
    * @param container container name
    * @param id        object id
    */
  def delete(container: String, id: String): Unit = {
    val con = account.getContainer(container)
    val obj = con.getObject(id)
    obj.delete()

    // get object list count in the container
    println(con.list().size())
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
  put("rahasak", "01101", blob)

  // get object
  get("rahasak", "01101")

  // delete object
  delete("rahasak", "01101")
}

