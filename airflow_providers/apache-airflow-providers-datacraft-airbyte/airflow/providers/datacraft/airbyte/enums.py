from strenum import StrEnum


class NamespaceDefinitionEnum(StrEnum):
    source = "source"
    destination = "destination"
    customformat = "customformat"


class TimeUnitEnum(StrEnum):
    minutes = "minutes"
    hours = "hours"
    days = "days"
    weeks = "weeks"
    months = "months"


class ConnectionStatusEnum(StrEnum):
    active = "active"
    inactive = "inactive"
    deprecated = "deprecated"


class NonBreakingChangesPreferenceEnum(StrEnum):
    ignore = "ignore"
    disable = "disable"
    propagate_columns = "propagate_columns"
    propagate_fully = "propagate_fully"


class JobStatusEnum(StrEnum):
    pending = "pending"
    running = "running"
    incomplete = "incomplete"
    failed = "failed"
    succeeded = "succeeded"
    canceled = "canceled"


class JobSyncConfigTypeEnum(StrEnum):
    check_connection_source = "check_connection_source"
    check_connection_destination = "check_connection_destination"
    discover_schema = "discover_schema"
    get_spec = "get_spec"
    sync = "sync"
    reset_connection = "reset_connection"
    refresh = "refresh"
    # clear = "clear"
