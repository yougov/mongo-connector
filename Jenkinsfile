podTemplate(
  label: "mongo-connector-build-pod",
  containers: [
    containerTemplate(name: 'jenkins-slave', image: 'jenkins/jnlp-slave:3.10-1', ttyEnabled: true, command: 'cat'),
    containerTemplate(name: 'python-build-container', image: 'python:3.5-jessie', command: 'cat', ttyEnabled: true),
    containerTemplate(name: 'docker', image: 'docker:1.12.6', command: 'cat', ttyEnabled: true),
  ],
  volumes:[
    hostPathVolume(mountPath: '/var/run/docker.sock', hostPath: '/var/run/docker.sock'),
  ]
)
{
  node("mongo-connector-build-pod") {
    container('jenkins-slave') {
      stage('Set build params') {
        def timeStamp = Calendar.getInstance().getTime().format('YYMMddHHmm',TimeZone.getTimeZone('UTC'))
        def imageTag = "${env.BRANCH_NAME}-${timeStamp}"
        env.IMAGE_TAG = imageTag
        currentBuild.displayName = "${env.IMAGE_TAG}"
        currentBuild.description = "Built from branch: ${env.BRANCH_NAME}"
      }
      stage('Clone repository') {
        checkout([
          $class: 'GitSCM',
          branches: [[name: "*/${env.BRANCH_NAME}"]],
          userRemoteConfigs: scm.userRemoteConfigs
        ])
      }
    }
    container('python-build-container') {
      stage('Build wheel') {
        sh './scripts/build.sh'
      }
    }
    container('docker') {
      def image
      stage('Build container') {
        // using the --pull argument forces the build process to pull the image

        def tag = "riffyninc/mongo-connector:${env.IMAGE_TAG}"
        image = docker.build(tag, "--pull .")
      }
      stage('Push container') {
        docker.withRegistry('https://registry.hub.docker.com', 'riffynbuild-dockerhub-credentials') {
          image.push()
          image.push("${env.BRANCH_NAME}-latest")
        }
      }
    }
  }
}
