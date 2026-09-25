from datetime import timedelta
from feast import (
    Entity,
    Field,
    FeatureView,
    FileSource,
    PushSource,
    ValueType,
)
from feast.types import Float64, Int64

user_entity = Entity(
    name="user_id",
    value_type=ValueType.STRING,
    join_keys=["user_id"],
    description="Unique identifier for the account user"
)

batch_source = FileSource(
    name="user_transaction_aggregates_batch",
    path="data/transaction_features.parquet",
    timestamp_field="datetime",
)

push_source = PushSource(
    name="user_transaction_push_source",
    batch_source=batch_source,
)

user_transaction_feature_view = FeatureView(
    name="user_transaction_feature_view",
    entities=[user_entity],
    ttl=timedelta(days=1),  # Set 1 day TTL to prevent early TTL eviction
    schema=[
        Field(name="tx_count_10m", dtype=Int64),
        Field(name="total_amount_10m", dtype=Float64),
    ],
    online=True,
    source=push_source,
)