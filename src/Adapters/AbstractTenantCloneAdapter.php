<?php

declare(strict_types=1);

namespace PuyuPe\SiproInternalApiCloneDb\Adapters;

use PuyuPe\SiproInternalApiCore\Contracts\Adapter\TenantCloneAdapterInterface;
use PuyuPe\SiproInternalApiCore\Contracts\Dto\TenantExportRequestDTO;
use PuyuPe\SiproInternalApiCore\Contracts\Dto\TenantExportResponseDTO;
use PuyuPe\SiproInternalApiCore\Contracts\Dto\TenantImportRequestDTO;
use PuyuPe\SiproInternalApiCore\Contracts\Dto\TenantImportResponseDTO;

abstract class AbstractTenantCloneAdapter implements TenantCloneAdapterInterface
{
    public function exportTenant(string $appKey, TenantExportRequestDTO $dto): TenantExportResponseDTO
    {
        $dbName = $this->resolveTenantDatabase($appKey, $dto->appKey, $dto->projectCode);

        $dumpDir = $this->dumpDir();
        $this->ensureDumpDir($dumpDir);

        $dumpPath = $this->buildDumpPath($dumpDir, $dbName, $dto->projectCode);

        $this->runDump($dbName, $dumpPath);
        $this->ensureDumpFileIsNotEmpty($dumpPath);

        $checksum = $this->checksum($dumpPath);

        return new TenantExportResponseDTO(
            appKey: $appKey,
            projectCode: $dto->projectCode,
            dumpPath: $dumpPath,
            checksum: $checksum,
            createdAt: gmdate('c')
        );
    }

    public function importTenant(string $appKey, TenantImportRequestDTO $dto): TenantImportResponseDTO
    {
        $dbName = $this->resolveTenantDatabase($appKey, $dto->appKey, $dto->projectCode);

        $dumpPath = $this->resolveDumpPath($dto->dumpPath);
        if ($dumpPath === null || !is_file($dumpPath)) {
            $this->throwValidationError('dumpPath', 'invalid', 'dumpPath is invalid or not found.');
        }

        $checksum = $this->checksum($dumpPath);
        if (strtolower($checksum) !== strtolower($dto->checksum)) {
            $this->throwValidationError('checksum', 'mismatch', 'dump checksum does not match.');
        }

        $this->createDatabaseIfMissing($dbName);
        $this->runRestore($dbName, $dumpPath);

        return new TenantImportResponseDTO(
            appKey: $appKey,
            projectCode: $dto->projectCode,
            database: $dbName,
            restored: true
        );
    }

    protected function buildDumpPath(string $dumpDir, string $dbName, string $projectCode): string
    {
        $fileName = sprintf(
            '%s_%s_%s.sql.gz',
            $dbName,
            $this->sanitizeSuffix($projectCode),
            gmdate('Ymd_His')
        );

        return rtrim($dumpDir, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . $fileName;
    }

    protected function ensureDumpDir(string $dumpDir): void
    {
        if (is_dir($dumpDir)) {
            return;
        }

        if (!mkdir($dumpDir, 0750, true) && !is_dir($dumpDir)) {
            $this->throwProvisionFailed('Failed to create dump directory.');
        }
    }

    protected function resolveDumpPath(string $dumpPath): ?string
    {
        $dumpDir = $this->dumpDir();
        $realDumpDir = realpath($dumpDir);
        $realDumpPath = realpath($dumpPath);

        if ($realDumpDir === false || $realDumpPath === false) {
            return null;
        }

        if (!str_starts_with($realDumpPath, $realDumpDir . DIRECTORY_SEPARATOR) && $realDumpPath !== $realDumpDir) {
            return null;
        }

        return $realDumpPath;
    }

    protected function checksum(string $path): string
    {
        $hash = hash_file('sha256', $path);

        if (!is_string($hash) || $hash === '') {
            $this->throwProvisionFailed('Failed to compute dump checksum.');
        }

        return $hash;
    }

    protected function sanitizeSuffix(string $value): string
    {
        $normalized = preg_replace('/[^a-z0-9_]+/i', '_', $value) ?? $value;
        $normalized = trim(strtolower($normalized), '_');
        return $normalized !== '' ? $normalized : 'tenant';
    }

    protected function runShellCommand(string $command, string $password, string $errorMessage): void
    {
        $env = $_ENV;
        if ($password !== '') {
            $env['MYSQL_PWD'] = $password;
        }

        $descriptorSpec = [
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];

        $process = proc_open(['/bin/bash', '-o', 'pipefail', '-c', $command], $descriptorSpec, $pipes, null, $env);

        if (!is_resource($process)) {
            $this->throwProvisionFailed($errorMessage);
        }

        $stdout = stream_get_contents($pipes[1]);
        $stderr = stream_get_contents($pipes[2]);

        fclose($pipes[1]);
        fclose($pipes[2]);

        $exitCode = proc_close($process);
        if ($exitCode !== 0) {
            $this->throwProvisionFailed($errorMessage, [
                'exit_code' => $exitCode,
                'stdout' => $stdout,
                'stderr' => $stderr,
            ]);
        }
    }

    protected function buildCreateDatabaseSql(string $dbName): string
    {
        $quoted = str_replace('`', '``', $dbName);
        $charset = $this->dbCharset();
        $collation = $this->dbCollation();

        return sprintf(
            'CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET %s COLLATE %s',
            $quoted,
            $charset,
            $collation
        );
    }

    protected function runDump(string $dbName, string $dumpPath): void
    {
        $command = sprintf(
            '%s --single-transaction --routines --triggers --events --skip-lock-tables -h %s -P %d -u %s %s | %s -c > %s',
            escapeshellarg($this->dumpBinary()),
            escapeshellarg($this->dbHost()),
            $this->dbPort(),
            escapeshellarg($this->dbUser()),
            escapeshellarg($dbName),
            escapeshellarg($this->gzipBinary()),
            escapeshellarg($dumpPath)
        );

        $this->runShellCommand($command, $this->dbPassword(), 'Failed to export tenant database.');
    }

    protected function runRestore(string $dbName, string $dumpPath): void
    {
        $command = sprintf(
            '%s -dc %s | %s -h %s -P %d -u %s %s',
            escapeshellarg($this->gzipBinary()),
            escapeshellarg($dumpPath),
            escapeshellarg($this->mysqlBinary()),
            escapeshellarg($this->dbHost()),
            $this->dbPort(),
            escapeshellarg($this->dbUser()),
            escapeshellarg($dbName)
        );

        $this->runShellCommand($command, $this->dbPassword(), 'Failed to import tenant database.');
    }

    protected function ensureDumpFileIsNotEmpty(string $dumpPath): void
    {
        if (!is_file($dumpPath)) {
            $this->throwProvisionFailed('Dump file was not created.');
        }

        $size = filesize($dumpPath);
        if ($size === false || $size === 0) {
            $this->throwProvisionFailed('Dump file is empty.');
        }
    }

    abstract protected function resolveTenantDatabase(string $pathAppKey, string $payloadAppKey, string $projectCode): string;

    protected function assertIdentity(string $pathAppKey, string $payloadAppKey, string $projectCode): void
    {
        if ($pathAppKey === '') {
            $this->throwValidationError('appKey', 'required', 'appKey is required.');
        }

        if ($payloadAppKey !== $pathAppKey) {
            $this->throwValidationError('appKey', 'mismatch', 'appKey does not match request payload.');
        }

        if ($projectCode === '') {
            $this->throwValidationError('projectCode', 'required', 'projectCode is required.');
        }
    }

    protected function resolveDatabaseName(string $expectedDbName, ?string $actualDbName): string
    {
        $dbName = $actualDbName ?? $expectedDbName;

        if ($dbName !== $expectedDbName) {
            $this->throwValidationError('projectCode', 'mismatch', 'projectCode does not match tenant database.');
        }

        return $dbName;
    }

    /**
     * @return object|null
     */
    abstract protected function loadTenantRecord(string $appKey): ?object;

    abstract protected function extractDatabaseName(object $record): ?string;

    abstract protected function dumpDir(): string;

    abstract protected function createDatabaseIfMissing(string $dbName): void;

    abstract protected function dbHost(): string;

    abstract protected function dbUser(): string;

    abstract protected function dbPort(): int;

    abstract protected function dbPassword(): string;

    abstract protected function dbCharset(): string;

    abstract protected function dbCollation(): string;

    abstract protected function dumpBinary(): string;

    abstract protected function mysqlBinary(): string;

    abstract protected function gzipBinary(): string;

    abstract protected function throwValidationError(string $field, string $code, string $message): void;

    /**
     * @param array<string, mixed> $details
     */
    abstract protected function throwProvisionFailed(string $message, array $details = []): void;
}
