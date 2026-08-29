Write-Host "=== Kafka Restore Starting ==="

# Restore data into volumes
# ./restore-kafka.ps1


$backupDir = "kafka-backup"
$brokers = @("kafka-1", "kafka-2", "kafka-3")

foreach ($broker in $brokers) {
    Write-Host "Restoring $broker ..."

    $volumeName = "$broker-data"
    $backupPath = "$backupDir\$broker-data/data"

    docker run --rm `
        -v "$volumeName:/data" `
        -v "${PWD}/$backupDir/$broker-data:/backup" `
        alpine sh -c "cp -r /backup/data/* /data/"
}

Write-Host "=== Restore Complete ==="
