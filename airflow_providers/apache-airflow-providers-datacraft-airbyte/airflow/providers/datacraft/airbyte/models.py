from typing import Optional

from pydantic import BaseModel

from .enums import (
    NamespaceDefinitionEnum,
    TimeUnitEnum,
    ConnectionStatusEnum,
    NonBreakingChangesPreferenceEnum,
    JobStatusEnum,
    JobSyncConfigTypeEnum,
)


class Model(BaseModel, extra="ignore"):
    """BaseModel that ignores extra fields"""

    ...


class BaseSourceDefinition(BaseModel):
    name: str
    dockerRepository: str
    dockerImageTag: str
    documentationUrl: str | None = None


class SourceDefinitionSpec(BaseSourceDefinition):
    sourceDefinitionId: str


class WorkspaceSpec(Model):
    workspaceId: str
    name: str
    slug: str | None = None


class ScheduleSpec(Model):
    units: int
    timeUnit: TimeUnitEnum


class ConnectionSpec(Model):
    connectionId: str
    name: str
    sourceId: str
    destinationId: str
    status: ConnectionStatusEnum
    schedule: ScheduleSpec | None = None
    nonBreakingChangesPreference: NonBreakingChangesPreferenceEnum | None = None
    namespaceDefinition: NamespaceDefinitionEnum | None = None
    namespaceFormat: str | None = None
    prefix: str | None = None
    syncCatalog: dict | None = None
    # TODO syncCatalog schema


class BaseDestinationDefinition(Model):
    name: str
    dockerRepository: str
    dockerImageTag: str
    documentationUrl: str | None = None


class DestinationDefinitionSpec(BaseDestinationDefinition):
    destinationDefinitionId: str
    # name: str
    # dockerRepository: str
    # dockerImageTag: str
    # documentationUrl: str | None = None


class DestinationSpec(Model):
    destinationId: str
    name: str


class AirByteJobSpec(Model):
    jobId: str
    status: JobStatusEnum
    jobType: str
    connectionId: str


class SourceSpec(Model):
    sourceId: str
    name: str
    connectionConfiguration: dict


class AirbyteAttemptSpec(Model):
    attempt: dict[str, object]


class StatsSpec(Model):
    recordsEmitted: int | str | None = None
    bytesEmitted: int | str | None = None
    stateMessagesEmitted: int | str | None = None
    bytesCommitted: int | str | None = None
    recordsCommitted: int | str | None = None
    estimatedRecords: int | str | None = None
    estimatedBytes: int | str | None = None


class StreamStatsSpec(Model):
    streamName: str
    streamNamespace: str | None = None
    stats: StatsSpec


class AirbyteAttemptsSpec(Model):
    """Using in JobListSpec"""

    id: int | str
    status: JobStatusEnum
    createdAt: int | str
    updatedAt: int | str
    endedAt: int | str | None = None
    bytesSynced: int | str | None = None
    recordsSynced: int | str | None = None
    totalStats: Optional[dict[str, int]]
    streamStats: Optional[list[StreamStatsSpec]]


class JobSyncSpec(Model):
    id: str | int
    configType: JobSyncConfigTypeEnum
    configId: str | int
    createdAt: str | int
    updatedAt: str | int
    startedAt: str | int
    status: JobStatusEnum
    attempts: list[AirbyteAttemptSpec]


class JobListSpec(Model):
    id: int | str
    jobType: JobSyncConfigTypeEnum  # In fact it is configType in Api docs
    status: JobStatusEnum
    configId: str
    attempts: list[AirbyteAttemptsSpec]


class TriggerSyncResponseSpec(Model):
    job: dict
    attempts: list[dict]
