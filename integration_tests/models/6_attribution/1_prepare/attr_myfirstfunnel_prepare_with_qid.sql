-- depends_on: {{ ref('full_events') }}
-- depends_on: {{ ref('graph_qid') }}
{{ etlcraft.attr(features_list=['ym','yd','appmetrica']) }}


