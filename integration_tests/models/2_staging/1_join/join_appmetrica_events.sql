-- depends_on: {{ ref('incremental_appmetrica_events_default_deeplinks') }}
-- depends_on: {{ ref('incremental_appmetrica_events_default_events') }}
-- depends_on: {{ ref('incremental_appmetrica_events_default_install') }}
-- depends_on: {{ ref('incremental_appmetrica_events_default_screen_view') }}
-- depends_on: {{ ref('incremental_appmetrica_events_default_sessions_starts') }}
{{ etlcraft.join() }}