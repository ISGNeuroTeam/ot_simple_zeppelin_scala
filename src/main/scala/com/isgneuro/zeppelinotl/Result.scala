package com.isgneuro.zeppelinotl

import com.isgneuro.otp.connector.Dataset
import org.apache.zeppelin.interpreter.InterpreterResult

case class Result(interpreterResult: InterpreterResult, dataset: Option[Dataset])
