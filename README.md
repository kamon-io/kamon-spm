SPM Integration    ![Build Status](https://travis-ci.org/kamon-io/kamon-spm.svg?branch=master)
==========================
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/kamon-io/Kamon?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.kamon/kamon-spm_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.kamon/kamon-spm_2.12)

Reporting Metrics to SPM
=======================

[SPM] is a proactive performance monitoring solution that provides anomaly detection, alerting, transaction tracing, network topology discovery and log correlation available in the Cloud and On Premises.

Installation
------------

[Sign up]. [Create 'Akka' app] in SPM. Get your SPM app token. Add `kamon-spm` dependency to your project.

### Getting Started

Kamon akka-spm module is currently available for Scala 2.10, 2.11 and 2.12.

Supported releases and dependencies are shown below.

| kamon-spm  | status | jdk  | scala            |
|:------:|:------:|:----:|------------------|
|  0.6.7 | stable | 1.8+ |  2.10, 2.11, 2.12  |
|  1.1.1 | stable | 1.8+ |  2.10, 2.11, 2.12  |
|  1.1.2 | stable | 1.8+ |  2.10, 2.11, 2.12  |

To get started with SBT, simply add the following to your `build.sbt`
file:

```scala
libraryDependencies += "io.kamon" %% "kamon-spm" % "1.1.2"
```

Configuration
-------------

SPM backend extension requires the property `kamon.spm.token` to be defined. SPM provides reports for `akka-actor`, `akka-router`, `akka-dispatcher` and `system-metrics` categories. By default all entities for given categories are included.

```
  kamon {
    spm {
        token = "[place-token-here]"
    }
    reporters = ["kamon.spm.SPMReporter"]
  }
  
  kamon.util.filters {
  
    "akka.tracked-actor" {
      includes = [ "**" ]
    }
  
    "akka.tracked-dispatcher" {
      includes = [ "**" ]
    }
  
    "akka.tracked-router" {
      includes = [ "**" ]
    }
  }
```

To see a full example of Kamon SPM Backend configuration look at [application.conf] with Kamon and SPM.

**Note:** By default this extension uses hostname resolved using `InetAddress.getLocalHost.getHostName`. However, hostname can be redefined using `kamon.spm.hostname-alias` property.

Visualisation
-------------

Overview:

<img class="img-responsive" src="http://kamon.io/assets/img/spm-module-overview.png">

Actors:

<img class="img-responsive" src="http://kamon.io/assets/img/spm-module-actors.png">

Dispatchers:

<img class="img-responsive" src="http://kamon.io/assets/img/spm-module-dispatchers.png">

Routers:

<img class="img-responsive" src="http://kamon.io/assets/img/spm-module-routers.png">

CPU:

<img class="img-responsive" src="http://kamon.io/assets/img/spm-module-cpu.png">


[SPM]: http://sematext.com/spm/index.html
[Sign up]: https://apps.sematext.com/users-web/register.do
[Create 'Akka' app]: https://apps.sematext.com/spm-reports/registerApplication.do
[Sample Akka app]: https://github.com/sematext/kamon-spm-example
[application.conf]: https://github.com/sematext/kamon-spm-example/blob/master/src/main/resources/application.conf
