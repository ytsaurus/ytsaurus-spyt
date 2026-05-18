package org.apache.spark.deploy.rest

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

object RestServletSupport330 {

  def wrapRestServlet(servlet: RestServletCompat): AnyRef = {
    new RestServlet {
      override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
        val result = servlet.processGet(req.getPathInfo, req.getParameter)
        if (result._2.isDefined) {
          resp.setStatus(result._2.get)
        }
        sendResponse(result._1, resp)
      }

      override def doPost(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
        val result = servlet.processPost(() => req.getInputStream)
        if (result._2.isDefined) {
          resp.setStatus(result._2.get)
        }
        sendResponse(result._1, resp)
      }
    }
  }
}
