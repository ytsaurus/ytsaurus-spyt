package org.apache.spark.deploy.rest

import jakarta.servlet.http.{HttpServletRequest, HttpServletResponse}


object RestServletSupport400 {
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
