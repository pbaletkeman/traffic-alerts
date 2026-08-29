Write-Host "=== Kafka Backup Starting ==="

# Backup your cluster
#  ./backup-kafka.ps1


# Create backup directory
$backupDir = "kafka-backup"
New-Item -ItemType Directory -Force -Path $backupDir | Out-Null

# Brokers to back up
$brokers = @("kafka-1", "kafka-2", "kafka-3")

foreach ($broker in $brokers) {
    Write-Host "Backing up $broker ..."

    $dataBackup = "$backupDir\$broker-data"
    New-Item -ItemType Directory -Force -Path $dataBackup | Out-Null

    docker cp "$broker:/var/lib/kafka/data" "$dataBackup"
}

Write-Host "=== Backup Complete ==="
Write-Host "Backup stored in: $backupDir"
