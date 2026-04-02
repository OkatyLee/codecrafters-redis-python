param(
    [ValidateSet("smoke", "read-heavy", "write-heavy", "replication", "pubsub")]
    [string]$Scenario = "smoke",
    [int]$Clients = 50,
    [int]$Requests = 20000,
    [int]$DataSize = 128,
    [int]$Publishers = 4,
    [string]$ComposeFile = "docker-compose.observability.yml"
)

$mode = ""
$tests = switch ($Scenario) {
    "smoke" {
        $mode = "redis-benchmark"
        "ping,set,get"
    }
    "read-heavy" {
        $mode = "redis-benchmark"
        if ($PSBoundParameters.ContainsKey("Clients") -eq $false) { $Clients = 100 }
        if ($PSBoundParameters.ContainsKey("Requests") -eq $false) { $Requests = 50000 }
        "ping,get,mget"
    }
    "write-heavy" {
        $mode = "redis-benchmark"
        if ($PSBoundParameters.ContainsKey("Clients") -eq $false) { $Clients = 100 }
        if ($PSBoundParameters.ContainsKey("Requests") -eq $false) { $Requests = 50000 }
        "set,incr,lpush,rpush"
    }
    "replication" {
        $mode = "replication"
        if ($PSBoundParameters.ContainsKey("Clients") -eq $false) { $Clients = 20 }
        if ($PSBoundParameters.ContainsKey("Requests") -eq $false) { $Requests = 500 }
        ""
    }
    "pubsub" {
        $mode = "pubsub"
        if ($PSBoundParameters.ContainsKey("Clients") -eq $false) { $Clients = 20 }
        if ($PSBoundParameters.ContainsKey("Requests") -eq $false) { $Requests = 500 }
        ""
    }
}

switch ($mode) {
    "redis-benchmark" {
        docker compose -f $ComposeFile up -d --wait app prometheus grafana
        if ($LASTEXITCODE -ne 0) {
            exit $LASTEXITCODE
        }

        docker compose -f $ComposeFile run --rm benchmark `
            -h app `
            -p 6379 `
            -t $tests `
            -c $Clients `
            -n $Requests `
            -d $DataSize
    }
    "replication" {
        docker compose -f $ComposeFile up -d --wait app app-replica prometheus grafana
        if ($LASTEXITCODE -ne 0) {
            exit $LASTEXITCODE
        }

        python scripts/perf/replication_benchmark.py `
            --master-host 127.0.0.1 `
            --master-port 6379 `
            --replica-host 127.0.0.1 `
            --replica-port 6380 `
            --messages $Requests `
            --concurrency $Clients `
            --payload-size $DataSize
    }
    "pubsub" {
        docker compose -f $ComposeFile up -d --wait app prometheus grafana
        if ($LASTEXITCODE -ne 0) {
            exit $LASTEXITCODE
        }

        python scripts/perf/pubsub_benchmark.py `
            --host 127.0.0.1 `
            --port 6379 `
            --subscribers $Clients `
            --publishers $Publishers `
            --messages $Requests `
            --payload-size $DataSize
    }
}

exit $LASTEXITCODE
