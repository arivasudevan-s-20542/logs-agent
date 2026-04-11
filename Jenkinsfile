// ─────────────────────────────────────────────────────────────────────────────
// CI/CD — zcop-log-agent (Go 1.22)
//
// Jenkins runs ON the VPS — deploy is a direct local cp + systemctl restart.
//
// Requires Go 1.22+ installed on the VPS and available in PATH.
//
// GitHub webhook:
//   Payload URL: http://<VPS_IP>:8080/github-webhook/
//   Content type: application/json  |  Event: Push
// ─────────────────────────────────────────────────────────────────────────────

pipeline {
    agent any

    options {
        buildDiscarder(logRotator(numToKeepStr: '10'))
        timestamps()
        timeout(time: 10, unit: 'MINUTES')
        disableConcurrentBuilds()
    }

    environment {
        INSTALL_DIR  = '/opt/zcop-log-agent'
        GOROOT       = '/var/lib/jenkins/go-sdk/go'
        PATH         = "/var/lib/jenkins/go-sdk/go/bin:${env.PATH}"
        GOPATH       = '/var/lib/jenkins/go'
        CGO_ENABLED  = '0'
        GOOS         = 'linux'
        GOARCH       = 'amd64'
    }

    triggers {
        githubPush()
    }

    stages {

        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Download dependencies') {
            steps {
                sh 'go mod download'
            }
        }

        stage('Vet') {
            steps {
                sh 'go vet ./...'
            }
        }

        stage('Build') {
            steps {
                sh 'go build -ldflags="-s -w" -o logagent ./cmd/logagent'
            }
        }

        stage('Deploy') {
            steps {
                sh '''
                    set -e
                    sudo mkdir -p "${INSTALL_DIR}"

                    # Stop gracefully before replacing the binary
                    sudo systemctl stop zcop-log-agent 2>/dev/null || true

                    sudo cp logagent "${INSTALL_DIR}/logagent"
                    sudo chmod +x "${INSTALL_DIR}/logagent"

                    sudo systemctl start zcop-log-agent
                    echo "zcop-log-agent deployed ✓"
                '''
            }
        }
    }

    post {
        success {
            echo "✅ zcop-log-agent build #${env.BUILD_NUMBER} deployed"
        }
        failure {
            echo "❌ zcop-log-agent build #${env.BUILD_NUMBER} failed"
        }
        always {
            cleanWs()
        }
    }
}
