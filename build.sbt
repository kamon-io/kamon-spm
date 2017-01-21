/* =========================================================================================
 * Copyright © 2013-2017 the kamon project <http://kamon.io/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */


val kamonCore         = "io.kamon"                  %%  "kamon-core"            % "0.6.6"
val asyncHttpClient   = "org.asynchttpclient"       %   "async-http-client"     % "2.0.25"
val sprayJson         = "io.spray"                  %%  "spray-json"            % "1.3.3"
val libThrift         = "org.apache.thrift"         %   "libthrift"             % "0.9.2"
val mockito           = "org.mockito"               %   "mockito-core"          % "2.6.3"


lazy val root = (project in file("."))
  .settings(name := "kamon-spm")
  .settings(Seq(scalaVersion := "2.12.1", crossScalaVersions := Seq("2.10.6", "2.11.8", "2.12.1")))
  .settings(
      libraryDependencies ++=
        compileScope(kamonCore, asyncHttpClient, sprayJson, libThrift) ++
        testScope(scalatest, akkaDependency("testkit").value, mockito, slf4jApi, slf4jnop))
