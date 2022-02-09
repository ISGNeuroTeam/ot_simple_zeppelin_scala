package com.isgneuro.zeppelinotl

import org.apache.zeppelin.interpreter.InterpreterContext
import org.apache.zeppelin.resource.{ LocalResourcePool, ResourcePool }

object InterpreterContextHelper {
  /*def setResourcePool(ctx: InterpreterContext, rp: ResourcePool): InterpreterContext = {
    new InterpreterContext(
      ctx.getNoteId,
      ctx.getParagraphId,
      ctx.getReplName,
      ctx.getParagraphTitle,
      ctx.getParagraphText,
      ctx.getAuthenticationInfo,
      ctx.getConfig,
      ctx.getGui,
      ctx.getNoteGui,
      ctx.getAngularObjectRegistry,
      rp,
      ctx.getRunners,
      ctx.out)
  }

  def setResourcePool(ctx: InterpreterContext): InterpreterContext = {
    val lrp = new LocalResourcePool("LocalResourcePool")
    setResourcePool(ctx, lrp)
  }*/
}
